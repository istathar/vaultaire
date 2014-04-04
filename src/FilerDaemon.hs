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

{-# LANGUAGE BangPatterns       #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PackageImports     #-}
{-# LANGUAGE RecordWildCards    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module FilerDaemon where

import Blaze.ByteString.Builder
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.MVar
import Control.Concurrent.STM.TChan
import Control.Monad
import "mtl" Control.Monad.Error ()
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.List (foldl')
import Data.List.NonEmpty (fromList)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Serialize (encode)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock
import GHC.Conc
import Network.BSD (getHostName)
import Options.Applicative
import System.Environment (getProgName)
import System.Posix.Process (getProcessID)
import System.Rados.Monadic (Pool)
import qualified System.Rados.Monadic as Rados
import System.ZMQ4.Monadic (runZMQ)
import qualified System.ZMQ4.Monadic as Zero
import Text.Printf

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Writer
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import Vaultaire.Persistence.Constants
import qualified Vaultaire.Persistence.ContentsObject as Contents
import Vaultaire.JournalFile (BlockName,BlockSize)
import qualified Vaultaire.JournalFile as Journal

data Options = Options {
    optGlobalDebug     :: !Bool,
    optGlobalWriters   :: !Int,
    optGlobalRateLimit :: !Int,
    optGlobalByteLimit :: !Int,
    argGlobalPoolName  :: !String,
    optGlobalUserName  :: !String,
    argBrokerHost      :: !String
}

data Storage = Storage {
    pendingWrites  :: !(Map Label Builder),
    pendingSources :: !(Map Origin (Set SourceDict)),
    pendingCount   :: !Int
}

-- key,value,units
type Telemetry = (String,String,String)

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
sanityCheck [] = Left "Empty burst"
sanityCheck ps =
  let
    (Origin o') = origin $ head ps
  in
    if S.null o'
        then Left "Empty origin value, discarding burst"
        else Right ps

loadContents
    :: Directory
    -> Origin
    -> Pool (Directory,Int)
loadContents d o =
  let
    known :: Map SourceDict ByteString
    known = getSourcesMap d o

    l = Contents.formObjectLabel o

  in do
    if Map.null known
        then do
            st <- Contents.readVaultObject l
            let d1 = insertIntoDirectory d o st
            let x  = Set.size st
            return (d1,x)
        else do
            return (d,0)


--
-- This takes *a* contents list, not *the* contents list, in other words
-- this is just conveying the SourceDicts that are "new".
--
updateContents
    :: Directory
    -> Origin
    -> Set SourceDict
    -> Pool (Directory,Int)
updateContents d o st =
  let
    known :: Map SourceDict ByteString
    known = getSourcesMap d o

    l = Contents.formObjectLabel o

    st1 = Map.keysSet known

    new = Set.foldl (\acc s -> if Set.member s st1
                                        then acc
                                        else Set.insert s acc) Set.empty st
    x = Set.size new
  in do
    if x > 0
        then do
            Contents.appendVaultSource l new
            let d1 = insertIntoDirectory d o new
            return (d1,x)
        else
            return (d,x)


global_lock = S.intercalate "_" [__EPOCH__, "global"]

journal_name = S.intercalate "_" [__EPOCH__, "journal"]

requestWrite :: MVar Storage -> Map Label Builder -> Origin -> Set SourceDict -> Int -> IO ()
requestWrite storage writes o new n0 = do
    (Storage pm sm n) <- takeMVar storage

    let pm2 = Map.foldlWithKey f pm writes

    let sm2 = case Map.lookup o sm of
                Just st -> Map.insert o (Set.union st new) sm
                Nothing -> Map.singleton o new

    let n1 = n0 + n

    putMVar storage (Storage pm2 sm2 n1)

  where
    f acc label encodedB = Map.insertWith mappend label encodedB acc


chooseBlocks :: Int -> HashMap BlockName BlockSize -> (HashMap BlockName BlockSize, Int)
chooseBlocks limit blocksm = 
    HashMap.foldlWithKey' f (HashMap.empty, 0) blocksm
  where
    f :: (HashMap BlockName BlockSize, Int) -> BlockName -> BlockSize -> (HashMap BlockName BlockSize, Int)
    f (!m, !accumulated) block size =
        if accumulated + size < limit
            then (HashMap.insert block size m, accumulated + size)
            else (m, accumulated)
                

filer
    :: ByteString
    -> ByteString
    -> Int
    -> Int
    -> MVar Directory
    -> MVar Storage
    -> TChan Telemetry
    -> MVar Metrics
    -> IO ()
filer pool' user' bytelimit simultaneous directory storage telemetry metrics =
    Rados.runConnect (Just user') (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool' $ forever $ do
            -- don't need to lock here; bufferd is only appending
            blocksm <- Journal.readJournalObject journal_name
            let (chosenm, size) = chooseBlocks bytelimit blocksm

            output telemetry "ingest" (printf "%d" size) "bytes"

            if size > 0
                then writeCycle chosenm
                else liftIO $ threadDelay 5000000

  where
    writeCycle chosenm = do
            let blocks = HashMap.keys chosenm

            forM_ blocks $ \block -> do
                bursts <- Journal.readBlockObject block

                liftIO $ Async.mapConcurrently (\message' -> do
                    case parseMessage message' of
                        Left err -> do
                            -- TODO write block name to some lost+found journal
                            output telemetry "error" err ""
                            return ()

                        Right ps -> do
                            -- temporary, replace with zmq message part
                            let n = length ps
                            output telemetry "worker" (printf "%d" n) "points"

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

                            requestWrite storage pm o st n
                        ) bursts


            t1 <- liftIO $ getCurrentTime

            Storage pm sm n <- liftIO $ takeMVar storage
            liftIO $ putMVar storage (Storage Map.empty Map.empty 0)
            let !labels = Map.size pm

            Metrics{..} <- liftIO $ takeMVar metrics
            liftIO $ putMVar metrics $ Metrics (metricWrites + n)

            output telemetry "writing" (printf "%d" n) "points"
            output telemetry "across"  (printf "%d" labels) "labels"

--
-- This is, obviously, a total hack. And horrible. But it is the necessary
-- guard until we roll out atomic compare and set care of the new operations
-- API in the next Ceph version.
--

            Rados.withSharedLock global_lock "name" "desc" "tag" (Just 60.0) $ do
                Bucket.appendVaultPoints simultaneous pm

                unless (Map.null sm) $ do
                    d1 <- liftIO $ takeMVar directory
                    let os = Map.toList sm

                    d2 <- foldM (\d (o,_) -> do
                            (d',x) <- loadContents d o
                            if x > 0 then output telemetry "loaded" (printf "%d" x) "sources" else return ()
                            return d'
                        ) d1 os

                    d3 <- foldM (\d (o,st) -> do
                            (d',x) <- updateContents d o st
                            if x > 0 then output telemetry "saved" (printf "%d" x) "sources" else return ()
                            return d'
                        ) d2 os

                    liftIO $ putMVar directory d3


            forM_ blocks $ \block -> do
                Journal.deleteBlockObject block
            
            Rados.withExclusiveLock journal_name "name" "desc" Nothing $ do
                blockm <- Journal.readJournalObject journal_name

                let blocksm' = HashMap.difference blockm chosenm

                Journal.writeJournalObject journal_name blocksm'


            t2 <- liftIO $ getCurrentTime

            let delta = diffUTCTime t2 t1
            let deltaFloat = (fromRational $ toRational delta) :: Float
            let deltaPadded = printf "%0.3f" deltaFloat
            output telemetry "delta" deltaPadded "seconds"

            let countFloat = (fromRational . toRational) n
            let rateFloat = countFloat / deltaFloat
            let ratePadded = printf "%0.1f" rateFloat
            output telemetry "rate" ratePadded "points/second"

            let lFloat = (fromRational . toRational) labels
            let lRateFloat = lFloat / deltaFloat
            let lRatePadded = printf "%0.1f" lRateFloat
            output telemetry "cluster" lRatePadded "labels/second"


output :: MonadIO ω => TChan (String,String,String) -> String -> String -> String -> ω ()
output telemetry k v u = liftIO $ atomically $ writeTChan telemetry (k, v, u)

parseMessage :: ByteString -> Either String [Point]
parseMessage y' = do
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
    new = foldl' g Set.empty ps


processBurst
    :: Directory
    -> Origin
    -> [Point]
    -> (Map Label Builder)
processBurst d o ps = build
  where
    known = getSourcesMap d o

    f :: Map Label Builder -> Point -> Map Label Builder
    f m0 p =
      let
        s  = source p
        s' = case Map.lookup s known of
            Just hash'  -> hash'
            Nothing     -> error "No hash found"

        (l,encodedB) = bucket o s' p
      in
        Map.insertWith mappend l encodedB m0

    build :: Map Label Builder
    build = foldl' f Map.empty ps


--
-- See documentation for System.ZMQ4.Monadic's async; apparently this is
-- arranged such that the runZMQ scope does not end until the child Asyncs do
-- (via reference counting)
--
comms :: String -> Bool -> TChan Telemetry -> MVar Metrics -> IO ()
comms broker d telemetry metrics = do
    linkThread $ runZMQ $ do
        (identifier, hostname) <- liftIO getIdentifierAndHostname

        tele <- Zero.socket Zero.Pub
        Zero.connect tele ("tcp://" ++ broker ++ ":5581")

        forever $ do
            (k,v,u) <- liftIO . atomically $ readTChan telemetry
            when d $ liftIO $ putStrLn $ printf "%-10s %-9s %s" (k ++ ":") v u
            let reply = [S.pack k, S.pack v, S.pack u, identifier, hostname]
            Zero.sendMulti tele (fromList reply)


    linkThread $ runZMQ $ do
        mtri <- Zero.socket Zero.Rep
        Zero.bind mtri "tcp://*:5569"

        forever $ do
            _ <- Zero.receive mtri
            Metrics{..} <- liftIO $ readMVar metrics
            Zero.send mtri [] $ S.pack ("writes:" ++ show metricWrites)
  where
    linkThread a = Async.async a >>= Async.link

    getIdentifierAndHostname = do
        hostname <- S.pack <$> getHostName
        pid  <- getProcessID
        name <- getProgName
        let identifier = S.pack (name ++ "/" ++ show pid)
        return (identifier, hostname)


program :: Options -> MVar () -> IO ()
program (Options d c s bytelimit pool user broker) quitV = do
    putStrLn $ "filerd starting"
 
    dV <- newMVar Map.empty
    telC <- newTChanIO
    storeV <- newMVar (Storage Map.empty Map.empty 0)
    metricV <- newMVar (Metrics 0)

    -- Startup communications threads for telemetry and metrics
    linkThread $ comms broker d telC metricV

    -- Startup writer thread
    replicateM_ c $
        linkThread $ filer (S.pack pool) (S.pack user) bytelimit s dV storeV telC metricV

    -- Our work here is done
    takeMVar quitV
    putStrLn "filerd stopping"
  where
    linkThread a = Async.async a >>= Async.link


toplevel :: Parser Options
toplevel = Options
    <$> switch
            (long "debug" <>
             short 'd' <>
             help "Write debug telemetry to stdout")
    <*> option
            (long "connections" <>
             short 'c' <>
             metavar "NUM" <>
             value 1 <>
             showDefault <>
             help "Number of workers with connections to Ceph concurrently")
    <*> option
            (long "objects" <>
             short 's' <>
             value 100 <>
             metavar "NUM" <>
             showDefault <>
             help "Number of objects being written to simultaneously")
    <*> option
            (long "bytelimit" <>
             short 'b' <>
             metavar "NUM" <>
             value 67108864 <>
             showDefault <>
             help "Size limit for inbound work, in bytes")
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


commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Ingestion worker to feed points into the vault" <>
                header "A data vault for metrics")
