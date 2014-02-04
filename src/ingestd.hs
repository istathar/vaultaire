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
{-# LANGUAGE BangPatterns     #-}
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
import System.ZMQ4.Monadic hiding (source)

import Vaultaire.Conversion.Receiver
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents


data Channels = Channels {
    inbound     :: !(MVar [ByteString]),
    acknowledge :: !(Chan [ByteString]),
    telemetry   :: !(Chan ByteString),
    storage     :: !(Chan (Origin, UTCTime, ByteString, [Point]))
}

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


writer
    :: ByteString
    -> Channels
    -> IO ()
writer pool' c =
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool pool' $ loop Map.empty
  where
    msgV = inbound c
    ackC = acknowledge c
    telC = telemetry c
    storeC = storage c
    delimiter' = S.empty

    loop cm = do
        (o', t1, envelope', ps) <- liftIO $ readChan storeC

        let st = processBurst cm o' ps

        let l = Contents.formObjectLabel o'
        cm2 <- withSharedLock (runLabel l) "name" "desc" "tag" (Just 10.0) $ do
            Bucket.appendVaultPoints o' ps

            if (Set.null st)
                then return cm
                else updateContents cm o' l st

        liftIO $ do
            writeChan ackC [envelope', delimiter', S.empty]

            t2 <- getCurrentTime
            let delta = diffUTCTime t2 t1
            sendTelemetries telC delta (length ps)

        loop cm2

    sendTelemetries chan delta num =
        let telems = [ "delta: " `showTelem` delta
                     , "num: "   `showTelem` num
                     ] in mapM_ (writeChan chan) telems

    showTelem prefix = (prefix `S.append`) . S.pack . show


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
    -> Label
    -> Set SourceDict
    -> Pool (Map Origin (Set SourceDict))
updateContents cm0 o' l new  =
  let
    st0 = Map.findWithDefault Set.empty o' cm0
  in do
    st1 <- if Set.null st0
        then do
            Contents.readVaultObject l
        else do
            return st0

    let st2 = Set.foldl (\acc s -> Set.insert s acc) st1 new

    if Set.size st2 > Set.size st1
        then do
            Contents.appendVaultSource l new
            return $ Map.insert o' st2 cm0
        else
            return cm0


processBurst
    :: Map Origin (Set SourceDict)
    -> Origin
    -> [Point]
    -> Set SourceDict
processBurst _ _ [] =
    Set.empty
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
  in
    new

worker :: Channels -> IO ()
worker c =
  let
    msgV = inbound c
    ackC = acknowledge c
    storeC = storage c
  in
    forever $ do
        [envelope', delimiter', message'] <- takeMVar msgV
        t1 <- getCurrentTime

        case parseMessage message' of
            Left err -> do
                writeChan ackC [envelope', delimiter', S.pack err]
            Right ps -> do
                -- temporary, replace with zmq message part
                let o' = origin $ head ps

                writeChan storeC (o', t1, envelope', ps)


--
-- See documentation for System.ZMQ4.Monadic's async; apparently this is
-- arranged such that the runZMQ scope does not end until the child Asyncs do.
--
receiver
    :: String
    -> Channels
    -> Bool
    -> IO ()
receiver broker c d =
  let
    msgV = inbound c
    ackC = acknowledge c
    telC = telemetry c
    storeC = storage c
  in
    runZMQ $ do
        work <- socket Pull
        connect work ("tcp://" ++ broker ++ ":5561")

        ackn <- socket Push
        connect ackn ("tcp://" ++ broker ++ ":5560")

        tele <- socket Pub
        bind tele "tcp://*:5570"

        a1 <- async $ forever $ do
            x' <- liftIO $ readChan telC
            when d $ liftIO $ S.putStrLn x'
            send tele [] x'

        linkThread a1

        a2 <- async $ forever $ do
            msg <- receiveMulti work
            liftIO $ putMVar msgV msg

        linkThread a2

        a3 <- async $ forever $ do
            as <- liftIO $ readChan ackC
            sendMulti ackn (fromList as)

        linkThread a3

        return ()
  where
    linkThread a = liftIO $ Async.link a
    {-# INLINE linkThread #-}


program :: Options -> IO ()
program (Options d w broker pool) = do
    -- Incoming requests are given to worker threads via the work mvar
    msgV <- newEmptyMVar

    -- Replies from worker threads come back via the ack mvar
    ackC <- newChan

    -- Telemetry streams over this channel
    telC <- newChan

    storeC <- newChan

    let c = Channels {
        inbound = msgV,
        acknowledge = ackC,
        telemetry = telC,
        storage = storeC
    }

    -- Initialize thread pool to requested size
    replicateM_ w $
        linkThread $ worker c

    -- Startup writer thread
    linkThread $ writer (S.pack pool) c


    -- Startup communications threads
    linkThread $ receiver broker c d

    -- Our work here is done
    goToSleep
  where
    linkThread a = Async.async a >>= Async.link
    {-# INLINE linkThread #-}

    goToSleep    = threadDelay maxBound >> goToSleep


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


main :: IO ()
main =
    execParser commandLineParser >>= program
