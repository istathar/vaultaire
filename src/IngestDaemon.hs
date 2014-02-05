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

module IngestDaemon where

import Blaze.ByteString.Builder
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
import Data.Serialize (encode)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock
import GHC.Conc
import Options.Applicative
import System.IO.Unsafe (unsafePerformIO)
import System.Rados
import System.ZMQ4.Monadic hiding (source)

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Writer
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents

type Envelope = ByteString

data Options = Options {
    optGlobalDebug   :: !Bool,
    optGlobalWorkers :: !Int,
    argBrokerHost    :: !String,
    argPoolName      :: !String
}


data Mutexes = Mutexes {
    inbound     :: !(MVar [ByteString]),
    acknowledge :: !(Chan [ByteString]),
    telemetry   :: !(Chan ByteString),
    storage     :: !(Chan (Origin, UTCTime, Int, Envelope, Set SourceDict, Map Label Builder)),
    contents    :: !(MVar Contents)
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
    -> Mutexes
    -> IO ()
writer pool' chan =
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool pool' $ loop nullContents
  where
    msgV = inbound chan
    ackC = acknowledge chan
    telC = telemetry chan
    storeC = storage chan
    delimiter' = S.empty

    loop c = do
        (o', t1, num, envelope', st, pm) <- liftIO $ readChan storeC

        let l = Contents.formObjectLabel o'

        c2 <- withSharedLock (runLabel l) "name" "desc" "tag" (Just 10.0) $ do
            Bucket.appendVaultPoints pm

            if (Set.null st)
                then return c
                else updateContents c o' l st

        liftIO $ do
            writeChan ackC [envelope', delimiter', S.empty]

            t2 <- getCurrentTime
            let delta = diffUTCTime t2 t1
            sendTelemetries telC delta num

        loop c2



    sendTelemetries chan delta num =
        let telems = [ "delta: " `showTelem` delta
                     , "points: "   `showTelem` num
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
    :: Contents
    -> Origin
    -> Label
    -> Set SourceDict
    -> Pool (Contents)
updateContents c o' l new  =
  let
    sm0 :: Map SourceDict ByteString
    sm0 = getSourcesMap c o'
    st0 = Map.keysSet sm0
  in do
    st1 <- if Set.null st0
        then do
            Contents.readVaultObject l
        else do
            return st0

    let st2 = Set.foldl (\acc s -> Set.insert s acc) st1 new -- FIXME add membership test

    if Set.size st2 > Set.size st1
        then do
            Contents.appendVaultSource l new

            return $ insertIntoContents c o' st2
        else
            return c


--
-- We express as a VaultPoint protobuf, serialize to bytes, then prepend a
-- VaultPrefix in order to store the size necessary to be able to read it back
-- again.
--

bucket :: Origin -> Point -> (Label, Builder)
bucket o' p =
  let
    p' = encodePoint p
    pB = fromByteString p'
    r  = createDiskPrefix (fromIntegral $ S.length p')
    r' = encode r
    rB = fromByteString r'

    bB = mappend rB pB

    s  = source p
    t  = timestamp p

    l = Bucket.formObjectLabel o' s t
  in
    (l,bB)


processBurst
    :: Contents
    -> Origin
    -> [Point]
    -> (Set SourceDict, Map Label Builder)
processBurst _ _  [] = (Set.empty, Map.empty)
processBurst c o' ps = (new, pm)
  where
    known :: Map SourceDict ByteString
    known = getSourcesMap c o'

    g :: Set SourceDict -> Point -> Set SourceDict
    g st p =
      let
        s = source p
      in
        if Map.member s known
            then st
            else Set.insert s st

    new :: Set SourceDict
    new = foldl g Set.empty ps


    f :: Map Label Builder -> Point -> Map Label Builder
    f m0 p =
      let
        (l,encodedB) = bucket o' p
      in
        Map.insertWith mappend l encodedB m0

    -- FIXME change seed to shared accumulator
    pm :: Map Label Builder
    pm = foldl f Map.empty ps



worker :: Mutexes -> IO ()
worker chan =
  let
    msgV = inbound chan
    ackC = acknowledge chan
    storeC = storage chan
    cV = contents chan
  in
    forever $ do
        [envelope', delimiter', message'] <- takeMVar msgV
        t1 <- getCurrentTime
        c  <- readMVar cV

        case parseMessage message' of
            Left err -> do
                writeChan ackC [envelope', delimiter', S.pack err]

            Right ps ->
              let
                -- temporary, replace with zmq message part
                o' = origin $ head ps

                (st, pm) = processBurst c o' ps
              in do
                writeChan storeC (o', t1, length ps, envelope', st, pm)


--
-- See documentation for System.ZMQ4.Monadic's async; apparently this is
-- arranged such that the runZMQ scope does not end until the child Asyncs do.
--
receiver
    :: String
    -> Mutexes
    -> Bool
    -> IO ()
receiver broker chan d =
  let
    msgV = inbound chan
    ackC = acknowledge chan
    telC = telemetry chan
    storeC = storage chan
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

    contentsV <- newMVar nullContents

    let u = Mutexes {
        inbound = msgV,
        acknowledge = ackC,
        telemetry = telC,
        storage = storeC,
        contents = contentsV
    }

    -- Initialize thread pool to requested size
    replicateM_ w $
        linkThread $ worker u

    -- Startup writer thread
    linkThread $ writer (S.pack pool) u


    -- Startup communications threads
    linkThread $ receiver broker u d

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
