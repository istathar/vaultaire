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
{-# LANGUAGE RecordWildCards    #-}

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

data Options = Options {
    optGlobalDebug   :: !Bool,
    optGlobalWorkers :: !Int,
    argBrokerHost    :: !String,
    argPoolName      :: !String
}

-- This is how the broker will know to route all the way back to the client,
-- and how the client will know which message we are referencing.
data Ident = Ident {
    envelope   :: !ByteString, -- handled for us by the router socket, opaque.
    messageID :: !ByteString  -- a uint16_t, but opaque to us.
}

data Ack = Ack {
    ident   :: Ident,
    failure :: !ByteString -- possible failure, empty for success
}

data Storage = Storage {
    storageOrigin  :: !Origin,
    storageTime    :: !UTCTime,
    storageCount   :: !Int,
    storageIdent   :: !Ident,
    storageSources :: !(Set SourceDict),
    storageLabels  :: !(Map Label Builder)
}




data Mutexes = Mutexes {
    inbound     :: !(MVar [ByteString]),
    acknowledge :: !(Chan Ack), 
    telemetry   :: !(Chan ByteString),
    storage     :: !(Chan Storage),
    directory   :: !(MVar Directory)
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

--
-- This takes *a* contents list, not *the* contents list, in other words
-- this is just conveying the SourceDicts that are "new".
--
updateContents
    :: Directory
    -> Origin
    -> Label
    -> Set SourceDict
    -> Pool (Directory)
updateContents d o' l st  =
  let
    known :: Map SourceDict ByteString
    known = getSourcesMap d o'

    st0 = Map.keysSet known
  in do
    st1 <- if Map.null known
        then do
            Contents.readVaultObject l
        else do
            return st0

    let new = Set.foldl (\acc s -> if Set.member s st1
                                        then acc
                                        else Set.insert s acc) Set.empty st

    if Set.size new > 0
        then do
            Contents.appendVaultSource l new
            return $ insertIntoDirectory d o' new
        else
            return d


writer
    :: ByteString
    -> Mutexes
    -> IO ()
writer pool' Mutexes{..} =
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool pool' $ loop
  where
    loop = do
        --(o', t1, num, envelope', st, pm) <- liftIO $ readChan storage
        Storage{..} <- liftIO $ readChan storage

        let l = Contents.formObjectLabel storageOrigin

        withSharedLock (runLabel l) "name" "desc" "tag" (Just 10.0) $ do
            Bucket.appendVaultPoints storageLabels

            unless (Set.null storageSources) $ do
                d1 <- liftIO $ takeMVar directory
                d2 <- updateContents d1 storageOrigin l storageSources
                liftIO $ putMVar directory d2


        liftIO $ do
            writeChan acknowledge (Ack storageIdent "")

            t2 <- getCurrentTime
            let delta = diffUTCTime t2 storageTime
            sendTelemetries telemetry delta storageCount

        loop


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
-- We express as a VaultPoint protobuf, serialize to bytes, then prepend a
-- VaultPrefix in order to store the size necessary to be able to read it back
-- again.
--

bucket :: Origin -> ByteString -> Point -> (Label, Builder)
bucket o' s' p =
  let
    p' = encodePoint p
    pB = fromByteString p'
    r  = createDiskPrefix (fromIntegral $ S.length p')
    r' = encode r
    rB = fromByteString r'

    bB = mappend rB pB

    t  = timestamp p

    l = Bucket.formObjectLabel o' s' t
  in
    (l,bB)


identifyUnknown
    :: Map SourceDict ByteString
    -> [Point]
    -> Set SourceDict
identifyUnknown known ps = new
  where
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


processBurst
    :: Directory
    -> Origin
    -> [Point]
    -> (Map Label Builder)
processBurst d o' ps = build
  where
    known = getSourcesMap d o'

    f :: Map Label Builder -> Point -> Map Label Builder
    f m0 p =
      let
        s  = source p
        s' = case Map.lookup s known of
            Just hash'  -> hash'
            Nothing     -> error "No hash found"

        (l,encodedB) = bucket o' s' p
      in
        Map.insertWith mappend l encodedB m0

    -- FIXME change seed to shared accumulator
    build :: Map Label Builder
    build = foldl f Map.empty ps


{-

            return $ insertIntoContents c o' st2
-}

worker :: Mutexes -> IO ()
worker u =
  let
    msgV = inbound u
    ackC = acknowledge u
    storeC = storage u
    dV = directory u
  in
    forever $ do
        [envelope', msg_id', message'] <- takeMVar msgV
        let ident = Ident envelope' msg_id'
        t1 <- getCurrentTime

        case parseMessage message' of
            Left err -> do
                writeChan ackC $ Ack ident (S.pack err)

            Right ps -> do
                -- temporary, replace with zmq message part
                let o' = origin $ head ps

                d1 <- readMVar dV

                let known = getSourcesMap d1 o'
                    st = identifyUnknown known ps
                    d2 = if Set.null st
                            then d1
                            else insertIntoDirectory d1 o' st

                    pm = processBurst d2 o' ps

--
-- we do NOT write d2 back to the MVar here. It means duplicate work for it to
-- happen again down in updateContents, but it's the only way to ensure that
-- the data in the Directory is actually on disk.
--

                writeChan storeC $ Storage o' t1 (length ps) ident st pm




--
-- See documentation for System.ZMQ4.Monadic's async; apparently this is
-- arranged such that the runZMQ scope does not end until the child Asyncs do.
--
receiver
    :: String
    -> Mutexes
    -> Bool
    -> IO ()
receiver broker Mutexes{..} d =
    runZMQ $ do
        router <- socket Router
        connect router ("tcp://" ++ broker ++ ":5561")

        tele <- socket Pub
        bind tele "tcp://*:5570"

        a1 <- async $ forever $ do
            x' <- liftIO $ readChan telemetry
            when d $ liftIO $ S.putStrLn x'
            send tele [] x'

        linkThread a1

        a2 <- async $ forever $ do
            msg <- receiveMulti router
            liftIO $ putMVar inbound msg

        linkThread a2

        a3 <- async $ forever $ do
            Ack ident failure <- liftIO $ readChan acknowledge
            let reply = [ envelope ident, messageID ident, failure ]
            sendMulti router (fromList reply)

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

    dV <- newMVar nullDirectory

    let u = Mutexes {
        inbound = msgV,
        acknowledge = ackC,
        telemetry = telC,
        storage = storeC,
        directory = dV
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
