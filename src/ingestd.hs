--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PackageImports     #-}

module Main where

import Codec.Compression.LZ4
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad
import "mtl" Control.Monad.Error ()
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock
import GHC.Conc
import Options.Applicative
import System.IO.Unsafe (unsafePerformIO)
import System.Rados
import System.ZMQ4.Monadic hiding (async, source)

import Vaultaire.Conversion.Receiver
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents


{-
groupBurst :: [Point] -> Either String (Map Origin [Point])
groupBurst [] = Left "Zero length burst, ignoring"
groupBurst ps =
    Right $ foldl f Map.empty ps
  where
    f :: Map Origin [Point] -> Point -> Map Origin [Point]
    f m p = Map.insertWith (\(x:[]) xs -> (x:xs)) (origin p) [p] m
-}

--
-- This will be refactored since the Origin value will soon be conveyed at
-- the ØMQ level, rather than the current hack of an environment variable
-- passed to libmarquise. But at the moment this is a simple enough check
-- to ensure we at least have a value.
--

sanityCheck :: [Point] -> Either String [Point]
sanityCheck ps =
  let
    o' = origin $ head ps
  in
    if S.null o'
        then Left "Empty origin value, discarding burst"
        else Right ps


processBurst
    :: Map Origin (Set SourceDict)
    -> Origin
    -> [Point]
    -> Pool (Set SourceDict)
processBurst _ _ [] =
    return Set.empty
processBurst cm o' ps =
  let
    known = Map.findWithDefault Set.empty o' cm

    new :: Set SourceDict
    new = foldl g Set.empty ps

    g :: Set SourceDict -> Point -> Set SourceDict
    g st p =
      let
        s = source p
      in
        if Set.member s known
            then st
            else Set.insert s st

  in do
    let l' = Contents.formObjectLabel o'
    withSharedLock l' "name" "desc" "tag" (Just 10.0) $
        -- returns the sources that are "new"
        Bucket.appendVaultPoints o' ps
    return new


parseMessage :: ByteString -> Either String [Point]
parseMessage message' = do
    y' <- case decompress message' of
        Just x' -> Right x'
        Nothing -> Left "Decompressing DataBurst failed"

    ps <- decodeBurst y'

    sanityCheck ps


--
-- This takes *a* contents list, not *the* contents list, in other words
-- this is just conveying the SourceDicts that are "new".
--
updateContents
    :: Map Origin (Set SourceDict)
    -> Origin
    -> Set SourceDict
    -> Pool (Map Origin (Set SourceDict))
updateContents cm0 o' new  =
  let
    st0 = Map.findWithDefault Set.empty o' cm0

    l' = Contents.formObjectLabel o'
  in do
    withSharedLock l' "name" "desc" "tag" (Just 25.0) $ do
        st1 <- if Set.null st0
            then do
                Contents.readVaultObject l'
            else do
                return st0

        let st2 = Set.foldl (\acc s -> Set.insert s acc) st1 new

        if Set.size st2 > Set.size st1
            then do
                Contents.appendVaultSource l' new
                return $ Map.insert o' st2 cm0
            else
                return cm0


main :: IO ()
main =
    execParser commandLineParser >>= program


worker :: ByteString -> MVar [ByteString] -> MVar [ByteString] -> MVar (Map Origin (Set SourceDict)) -> Chan ByteString -> IO ()
worker pool' work ack cm_mvar telem_chan = do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool pool' $ forever $ do
            [envelope', delimiter', message'] <- liftIO $ takeMVar work
            t1 <- liftIO getCurrentTime

            cm <- liftIO $ readMVar cm_mvar

            (ok', o', st, num) <- case parseMessage message' of
                Left err -> do
                    return $ (S.pack err, S.empty, Set.empty, 0)
                Right ps -> do
                    -- temporary, replace with zmq message part
                    let o' = origin $ head ps

                    st <- processBurst cm o' ps
                    return $ (S.empty, o', st, length ps)

            unless (Set.null st) $ do
                liftIO $ putStrLn "Updating origin sets"
                cm1 <- liftIO $ takeMVar cm_mvar
                cm2 <- updateContents cm1 o' st
                liftIO $ putMVar cm_mvar cm2

            liftIO $ do
                t2 <- getCurrentTime
                let delta = diffUTCTime t2 t1
                sendTelemetries telem_chan delta num (S.length message')
                putMVar ack [envelope', delimiter', ok']

  where
    sendTelemetries chan delta num size =
        let telems = [ "delta: " `showTelem` delta
                     , "num: "   `showTelem` num
                     , "size: "  `showTelem` size
                     ] in mapM_ (writeChan chan) telems

    showTelem prefix = (prefix `S.append`) . S.pack . show


transmitTelemetries :: Chan ByteString -> IO ()
transmitTelemetries chan = do
    runZMQ $ do
        pub_sock <- socket Pub
        bind pub_sock "tcp://*:5570"

        forever $
            (liftIO $ readChan chan) >>= send pub_sock []


recieveWork :: String -> MVar [ByteString] -> IO ()
recieveWork broker mvar=
    runZMQ $ do
        pull_sock <- socket Pull
        connect pull_sock ("tcp://" ++ broker ++ ":5561")

        forever $ do
            [incoming] <- poll (-1) [Sock pull_sock [In] Nothing]
            when (In `elem` incoming) $ do
                msg <- receiveMulti pull_sock
                liftIO $ putMVar mvar msg

transmitAcks :: String -> MVar [ByteString] -> IO ()
transmitAcks broker mvar =
    runZMQ $ do
        push_sock <- socket Push
        connect push_sock  ("tcp://" ++ broker ++ ":5560")
        forever $
            (liftIO $ takeMVar mvar) >>= sendMulti push_sock . fromList


program :: Options -> IO ()
program (Options d w broker pool) = do
    -- Incoming requests are given to worker threads via the work mvar
    work_mvar <- newEmptyMVar
    -- Replies from worker threads come back via the ack mvar
    ack_mvar <- newEmptyMVar

    cm_mvar <- newMVar Map.empty

    -- Telemetries stream over this channel
    telem_chan <- newChan

    -- Initialize thread pool to requested size
    replicateM_ w $
        linkThread $ worker (S.pack pool) work_mvar ack_mvar cm_mvar telem_chan

    -- Sometimes we want to print telemetries
    when d (linkThread $ printTelemetry)

    -- And begin transmitting telemetries
    linkThread $ transmitTelemetries telem_chan

    -- Now read in work
    linkThread $ transmitAcks broker ack_mvar

    -- Our work here is done
    goToSleep
  where
    linkThread a = Async.async a >>= Async.link

    goToSleep    = threadDelay maxBound >> goToSleep


printTelemetry :: IO ()
printTelemetry = do
    runZMQ $ do
        telem <- socket Sub
        connect telem  ("tcp://127.0.0.1:5570")
        subscribe telem ""

        forever $ do
            message' <- receive telem
            liftIO $ S.putStrLn message'
--
-- Handle command line arguments properly. Copied from original
-- implementation in vault.hs
--

data Options = Options {
    optGlobalDebug   :: Bool,
    optGlobalWorkers :: Int,
    argBrokerHost    :: String,
    argPoolName      :: String
}


toplevel :: Parser Options
toplevel = Options
    <$> switch
            (long "debug" <>
             short 'd' <>
             help "Write debug telemetry to stdout")
    <*> option
            (long "workers" <>
             short 'w' <>
             value num <>
             showDefault <>
             help "Number of bursts to process simultaneously")
    <*> argument str
            (metavar "BROKER" <>
             help "Host name or IP address of broker to pull from")
    <*> argument str
            (metavar "POOL" <>
             help "Name of the Ceph pool metrics will be written to")
  where
    num = unsafePerformIO $ GHC.Conc.getNumCapabilities



commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Ingestion worker to feed points into the vault" <>
                header "A data vault for metrics")



