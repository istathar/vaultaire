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
{-# OPTIONS -fno-warn-type-defaults #-}

module IngestDaemon where

import Blaze.ByteString.Builder
import Codec.Compression.LZ4
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad
import "mtl" Control.Monad.Error ()
import Control.Monad.IO.Class
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
import System.Rados (Pool)
import qualified System.Rados as Rados
import qualified System.ZMQ4.Monadic as Zero
import Text.Printf

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Writer
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import Vaultaire.Persistence.Constants
import qualified Vaultaire.Persistence.ContentsObject as Contents

data Options = Options {
    optGlobalDebug    :: !Bool,
    optGlobalWorkers  :: !Int,
    argGlobalPoolName :: !String,
    optGlobalUserName :: !String,
    argBrokerHost     :: !String
}

-- This is how the broker will know to route all the way back to the client,
-- and how the client will know which message we are referencing.
data Ident = Ident {
    envelope  :: !ByteString, -- handled for us by the router socket, opaque.
    mystery   :: !ByteString, -- handled for us by the router socket, opaque.
    messageID :: !ByteString  -- a uint16_t, but opaque to us.
}

data Ack = Ack {
    ident   :: Ident,
    failure :: !ByteString -- possible failure, empty for success
}

data Storage = Storage {
    pendingWrites  :: !(Map Label Builder),
    pendingSources :: !(Map Origin (Set SourceDict)),
    pendingAcks    :: ![Ack],
    pendingCount   :: !Int
}

data Mutexes = Mutexes {
    inbound     :: !(MVar [ByteString]),
    acknowledge :: !(Chan Ack),
    telemetry   :: !(Chan (String,String,String)),
    storage     :: !(MVar Storage),
    pending     :: !(MVar ()),
    directory   :: !(MVar Directory),
    metrics     :: !(MVar Metrics)
}

data Metrics = Metrics {
    metricWrites :: Int
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
    (Origin o') = origin $ head ps
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
    -> Set SourceDict
    -> Pool (Directory)
updateContents d o st  =
  let
    known :: Map SourceDict ByteString
    known = getSourcesMap d o

    l' = Contents.formObjectLabel o

    st0 = Map.keysSet known
  in do
    st1 <- if Map.null known
        then do
            Contents.readVaultObject l'
        else do
            return st0

    let new = Set.foldl (\acc s -> if Set.member s st1
                                        then acc
                                        else Set.insert s acc) Set.empty st

    if Set.size new > 0
        then do
            Contents.appendVaultSource l' new
            return $ insertIntoDirectory d o new
        else
            return d


global_lock = S.intercalate "_" [__EPOCH__, "global"]

writer
    :: ByteString
    -> ByteString
    -> Mutexes
    -> IO ()
writer pool' user' Mutexes{..} =
    Rados.runConnect (Just user') (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool' $ forever $ do
            -- block until signalled to wake up
            liftIO $ takeMVar pending

            t1 <- liftIO $ getCurrentTime

            Storage pm sm as n <- liftIO $ takeMVar storage
            liftIO $ putMVar storage (Storage Map.empty Map.empty [] 0)

            Metrics{..} <- liftIO $ takeMVar metrics
            liftIO $ putMVar metrics $ Metrics (metricWrites + n)

            output telemetry "writing" (printf "%5d" n) ""

--
-- This is, obviously, a total hack. And horrible. But it is the necessary
-- guard until we roll out atomic compare and set care of the new operations
-- API in the next Ceph version.
--

            Rados.withSharedLock global_lock "name" "desc" "tag" (Just 60.0) $ do
                Bucket.appendVaultPoints pm

                unless (Map.null sm) $ do
                    d1 <- liftIO $ takeMVar directory
                    let os = Map.toList sm
                    d2 <- foldM (\d (o',st) -> updateContents d o' st) d1 os
                    liftIO $ putMVar directory d2


            liftIO $ mapM_ (writeChan acknowledge) as

            t2 <- liftIO $ getCurrentTime

            let delta = diffUTCTime t2 t1
            let deltaFloat = (fromRational $ toRational delta) :: Float
            let deltaPadded = printf "%9.3f" deltaFloat
            output telemetry "delta" deltaPadded "s"

            let countFloat = (fromRational . toRational) n
            let rateFloat = countFloat / deltaFloat
            let ratePadded = printf "%7.1f" rateFloat
            output telemetry "rate" ratePadded "p/s"


output :: MonadIO ω => Chan (String,String,String) -> String -> String -> String -> ω ()
output telemetry k v u = liftIO $ do
    writeChan telemetry (k, v, u)

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

    build :: Map Label Builder
    build = foldl f Map.empty ps


worker :: Mutexes -> IO ()
worker Mutexes{..} =
    forever $ do
        [envelope', mystery', msg_id', message'] <- takeMVar inbound
        let ident = Ident envelope' mystery' msg_id'

        case parseMessage message' of
            Left err -> do
                writeChan acknowledge $ Ack ident (S.pack err)

            Right ps -> do
                -- temporary, replace with zmq message part
                let n = length ps
                output telemetry "worker" (printf "%5d" n) ""

                let o = origin $ head ps

                d1 <- readMVar directory

                let known = getSourcesMap d1 o
                let st = identifyUnknown known ps
                let d2 = if Set.null st
                            then d1
                            else insertIntoDirectory d1 o st
--
-- (we do NOT write d2 back to the MVar here. It means duplicate work for it to
-- happen again down in updateContents, but it's the only way to ensure that
-- the data in the Directory is actually on disk)
--
                let pm = processBurst d2 o ps

                requestWrite storage pm o st (Ack ident S.empty) n
                void $ tryPutMVar pending ()


requestWrite :: MVar Storage -> Map Label Builder -> Origin -> Set SourceDict -> Ack -> Int -> IO ()
requestWrite storage writes o new a n0 = do
    (Storage pm sm as n) <- takeMVar storage

    let pm2 = Map.foldlWithKey f pm writes

    let sm2 = case Map.lookup o sm of
                Just st -> Map.insert o (Set.union st new) sm
                Nothing -> Map.singleton o new

    let n1 = n0 + n

    putMVar storage Storage { pendingWrites  = pm2
                            , pendingSources = sm2
                            , pendingAcks    = (a:as)
                            , pendingCount   = n1 }

  where
    f acc label encodedB = Map.insertWith mappend label encodedB acc





--
-- See documentation for System.ZMQ4.Monadic's async; apparently this is
-- arranged such that the runZMQ scope does not end until the child Asyncs do
-- (via reference counting)
--
receiver
    :: String
    -> Mutexes
    -> Bool
    -> IO ()
receiver broker Mutexes{..} d =
    Zero.runZMQ $ do
        router <- Zero.socket Zero.Router
        Zero.setReceiveHighWM (Zero.restrict 0) router
        Zero.connect router ("tcp://" ++ broker ++ ":5561")

        tele <- Zero.socket Zero.Pub
        Zero.bind tele "tcp://*:5570"

        mtri <- Zero.socket Zero.Rep
        Zero.bind mtri "tcp://*:5571"

        linkThread . forever $ do
            (k,v,u) <- liftIO $ readChan telemetry
            when d $ liftIO $ putStrLn $ printf "%-10s %-9s %s" (k ++ ":") v u
            let reply = [S.pack k, S.pack v, S.pack u]
            Zero.sendMulti tele (fromList reply)

        linkThread . forever $ do
            msg <- Zero.receiveMulti router
            liftIO $ putMVar inbound msg

        linkThread . forever $ do
            Ack ident failure <- liftIO $ readChan acknowledge
            let reply = [ envelope ident, mystery ident, messageID ident, failure ]
            Zero.sendMulti router (fromList reply)

        linkThread . forever $ do
            _ <- Zero.receive mtri
            Metrics{..} <- liftIO $ (readMVar metrics)

            let reply = [ S.pack ("writes:" ++ show metricWrites)]

            Zero.sendMulti mtri (fromList reply)
  where
    linkThread a = Zero.async a >>= liftIO . Async.link


program :: Options -> MVar () -> IO ()
program (Options d w pool user broker) quit_mvar = do
    -- Incoming requests are given to worker threads via the work mvar
    msgV <- newEmptyMVar

    -- Replies from worker threads come back via the ack mvar
    ackC <- newChan

    -- Telemetry streams over this channel
    telC <- newChan

    storeV <- newMVar (Storage Map.empty Map.empty [] 0)

    pendingV <- newEmptyMVar

    dV <- newMVar Map.empty

    metricsV <- newMVar (Metrics 0)

    let u = Mutexes {
        inbound = msgV,
        acknowledge = ackC,
        telemetry = telC,
        storage = storeV,
        pending = pendingV,
        directory = dV,
        metrics = metricsV
    }

    -- Initialize thread pool to requested size
    replicateM_ w $
        linkThread $ worker u

    -- Startup writer thread
    linkThread $ writer (S.pack pool) (S.pack user) u


    -- Startup communications threads
    linkThread $ receiver broker u d

    -- Our work here is done
    takeMVar quit_mvar

    -- TODO, graceful shutdown
    putStrLn "vaultaire stopping"
  where
    linkThread a = Async.async a >>= Async.link


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
    <*> strOption
            (long "pool" <>
             short 'p' <>
             metavar "POOL" <>
             value "vaultaire" <>
             showDefault <>
             help "Name of the Ceph pool metrics will be written to")
    <*> strOption
            (long "user" <>
             short 'u' <>
             metavar "USER" <>
             value "vaultaire" <>
             showDefault <>
             help "Username to use when authenticating to the Ceph cluster")
    <*> argument str
            (metavar "BROKER" <>
             help "Host name or IP address of broker to pull from")
  where
    num = unsafePerformIO $ getNumCapabilities



commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Ingestion worker to feed points into the vault" <>
                header "A data vault for metrics")
