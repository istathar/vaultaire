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

{-# LANGUAGE CPP                #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PackageImports     #-}
{-# LANGUAGE RecordWildCards    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module ReaderDaemon
(
    readerProgram,
    readerCommandLineParser,

    -- for testing
    demoWave
)
where

import Codec.Compression.LZ4
import Control.Applicative
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad
import "mtl" Control.Monad.Error ()
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import Data.List.Split (chunksOf)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Time.Clock
import GHC.Conc (getNumCapabilities)
import Network.BSD (getHostName)
import Options.Applicative hiding (reader)
import Pipes
import System.Environment (getProgName)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)
import qualified System.Rados.Monadic as Rados
import qualified System.ZMQ4.Monadic as Zero
import Text.Printf

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents

#include "config.h"

data Options = Options {
    optGlobalDebug    :: !Bool,
    optGlobalWorkers  :: !Int,
    argGlobalPoolName :: !String,
    optGlobalUserName :: !String,
    argBrokerHost     :: !String,
    optParallelReads  :: !Int
}

data Reply = Reply {
    envelope :: !ByteString, -- handled for us by the ROUTER socket, opaque.
    client   :: !ByteString, -- handled for us by the ROUTER socket, opaque.
    response :: !ByteString
}

data Mutexes = Mutexes {
    inbound     :: !(MVar [ByteString]),
    outbound    :: !(Chan Reply),
    telemetry   :: !(Chan (String,String,String)),
    directory   :: !(MVar Directory),
    contentsIn  :: !(MVar [ByteString]),
    contentsOut :: !(Chan Reply),
    radosLock   :: !(MVar Bool)
}


parseRequestMessage :: Origin -> ByteString -> Either String [Request]
parseRequestMessage o message' =
    decodeRequestMulti o message'


output :: MonadIO ω => Chan (String,String,String) -> String -> String -> String -> ω ()
output telemetry k v u = liftIO $ do
    writeChan telemetry (k, v, u)


reader
    :: ByteString
    -> ByteString
    -> Int
    -> Mutexes
    -> IO ()
reader pool' user' n_threads Mutexes{..} = do
    _ <- liftIO $ takeMVar radosLock
    Rados.runConnect (Just user') (Rados.parseConfig "/etc/ceph/ceph.conf") $ do
        liftIO $ putMVar radosLock True
        Rados.runPool pool' $ forever $ do
            [envelope', client', origin', request'] <- liftIO $ takeMVar inbound
            a1 <- liftIO $ getCurrentTime

            case parseRequestMessage (Origin origin') request' of
                Left err -> do
                    output telemetry "error" (show err) ""
                Right requests -> do
                    -- We have a valid request, so create a work pool of these
                    -- requests. This work pool is request_q
                    output telemetry "request" (printf "%d" (length requests)) "ranges"

                    request_q <- liftIO $ newMVar requests

                    -- Spawn an appropriate number of worker threads, each of
                    -- these will stream results back to the client over the
                    -- supplied channel within the context of their respective
                    -- requests.
                    threads <- replicateM n_threads $ Rados.async $
                        processRequests (envelope', client') request_q

                    liftIO $ mapM_ Async.wait threads

            a2 <- liftIO $ getCurrentTime
            let delta = diffUTCTime a2 a1
            let deltaFloat = (fromRational $ toRational delta) :: Float
            let deltaPadded = printf "%9.3f" deltaFloat
            output telemetry "duration" deltaPadded "seconds"

            liftIO $ writeChan outbound (Reply envelope' client' S.empty)

  where
    -- | For each request, stream points back.
    processRequests :: (ByteString, ByteString) -> MVar [Request] -> Rados.Pool ()
    processRequests ident@(envelope, client) request_q = do
        work <- liftIO $ popQ request_q
        case work of
            Just request -> do
                runEffect $ for (bucketTimemarks request
                                    >-> chunk 16
                                    >-> retrieveTimemarks request
                                    >-> filterRange request
                                    >-> pleaseEncodePoints
                                    >-> tryCompress)
                                (lift . liftIO . writeChan outbound . Reply envelope client)
                processRequests ident request_q
            Nothing -> return ()

    popQ :: MVar [a] -> IO (Maybe a)
    popQ m = do
        l <- takeMVar m
        case l of
            (x:xs) -> do
                putMVar m xs
                return $ Just x
            [] -> do
                putMVar m []
                return Nothing

    -- |
    -- Produce time marks from a request
    bucketTimemarks :: Request -> Producer [Timemark] Rados.Pool ()
    bucketTimemarks Request{..} = do
        yield $ Bucket.calculateTimemarks requestAlpha requestOmega

    -- |
    -- Split time marks up into manageable chunks as to not blow all our memory
    chunk :: Int -> Pipe [Timemark] [Timemark] Rados.Pool ()
    chunk n = forever $
        (chunksOf n <$> await) >>= mapM yield

    -- |
    -- Retrieve a chunk of time marks from the vault and retrieve the Map
    -- Timestamp Point associated with that range, these must be yielded in
    -- chronological order.
    retrieveTimemarks :: Request -> Pipe [Timemark] (Map Timestamp Point) Rados.Pool ()
    retrieveTimemarks Request{..} = forever $ do
        work <- await
        maps <- lift $ do
            async_replies <- forM work $ \i -> Rados.async $ do
                -- readVaultObject should probably be changed to expose a rados
                -- AsyncRequest directly. Untill then we use a
                -- Control.Concurrent.Async
                if requestOrigin == Origin "BENHUR"
                    then return $ demoWave requestOrigin i
                    else Bucket.readVaultObject requestOrigin requestSource i
            liftIO $ mapM Async.wait async_replies
        mapM yield $ filter (not . Map.null) maps

    filterRange :: Request -> Pipe (Map Timestamp Point) [Point] Rados.Pool ()
    filterRange Request{..} = forever $
            (Bucket.pointsInRange requestAlpha requestOmega <$> await) >>= yield


    pleaseEncodePoints :: Pipe [Point] ByteString Rados.Pool ()
    pleaseEncodePoints = forever $
        (encodePoints <$> await) >>= yield

    tryCompress :: Pipe ByteString ByteString Rados.Pool ()
    tryCompress = forever $ do
        msg <- await
        maybe (return ()) yield (compress msg)

demoWave
    :: Origin
    -> Timemark
    -> Map Timestamp Point
demoWave o i0 =
    let
        marks = [i0, i0 + 5 .. i0 + 99999]
        is = map fromIntegral marks

        period = 3600 * 3
        f = 1/period                                    -- instances per second
        w = 2 * pi * f
        y t = sin (w * ((fromRational . toRational) t))

        createPoint t v = Point {
                            origin = o,
                            source = SourceDict $ Map.fromList [("wave","sine")],
                            timestamp = t,
                            payload = Measurement v
                        }

        insertPoint acc i =
            let
                t = i * 1000000000
            in
                Map.insert t (createPoint t (y i)) acc
    in
        foldl insertPoint Map.empty is


contentsReader :: ByteString -> ByteString -> Mutexes -> IO ()
contentsReader pool user Mutexes{..} = do
    _ <- liftIO $ takeMVar radosLock
    Rados.runConnect (Just user) (Rados.parseConfig "/etc/ceph/ceph.conf") $ do
        liftIO $ putMVar radosLock True
        Rados.runPool pool $ forever $ do
            [envelope, client, _, request] <- liftIO $ takeMVar contentsIn
            d <- liftIO $ takeMVar directory
            let origin = Origin request
            --
            -- Check if we need to return the demo source, otherwise
            -- return an actual contents list.
            --
            burst <- if origin == (Origin "BENHUR")
                then do
                    let ds = (SourceDict (Map.fromList [("wave", "sine")]))
                    let ds' = [createSourceResponse ds]
                    return $ encodeSourceResponseBurst $ createSourceResponseBurst ds'
                else do
                    let lbl = Contents.formObjectLabel origin
                    st <- Contents.readVaultObject lbl
                    let d' = insertIntoDirectory d origin st
                    liftIO $ putMVar directory d'
                    let origin_sources o m = (Map.keys (Map.findWithDefault Map.empty o m))
                    let sources = map createSourceResponse (origin_sources origin d')
                    return $ encodeSourceResponseBurst (createSourceResponseBurst sources)
            liftIO $ writeChan contentsOut (Reply envelope client burst)

receiver
    :: String
    -> Mutexes
    -> Bool
    -> IO ()
receiver broker Mutexes{..} d = do
    host <- getHostName
    name <- getProgName
    pid  <- getProcessID
    let identifier' = S.pack (name ++ "/" ++ (show pid))
    let hostname'   = S.pack host

    Zero.runZMQ $ do
        router <- Zero.socket Zero.Router
        Zero.setReceiveHighWM (Zero.restrict 0) router
        Zero.connect router ("tcp://" ++ broker ++ ":5571")

        tele <- Zero.socket Zero.Pub
        Zero.connect tele ("tcp://" ++ broker ++ ":5581")

        contentsRouter <- Zero.socket Zero.Router
        Zero.connect contentsRouter ("tcp://" ++ broker ++ ":5573")

--
-- telemetry
--

        linkThread . forever $ do
            (k,v,u) <- liftIO $ readChan telemetry
            when d $ liftIO $ printf "%-10s %-9s %s" (k ++ ":") v u
            let reply = [S.pack k, S.pack v, S.pack u, identifier', hostname']
            Zero.sendMulti tele (fromList reply)

--
-- inbound work
--

        linkThread . forever $ do
            msg <- Zero.receiveMulti router
            liftIO $ case length msg of
                4 -> putMVar inbound msg
                n -> putStrLn $ "debug: illegal request received, n="++(show n)++"; ignoring"

--
-- send responses
--

        linkThread . forever $ do
            Reply{..} <- liftIO $ readChan outbound
            let reply = [envelope, client, response]
            Zero.sendMulti router (fromList reply)

--
-- get and handle contents requests
--

        linkThread . forever $ do
            msg <- Zero.receiveMulti contentsRouter
            liftIO $ putMVar contentsIn msg

        linkThread . forever $ do
            Reply{..} <- liftIO $ readChan contentsOut
            let reply = [envelope, client, (S.pack ""), response]
            Zero.sendMulti contentsRouter (fromList reply)
  where

    linkThread a = Zero.async a >>= liftIO . Async.link


readerProgram :: Options -> MVar () -> IO ()
readerProgram (Options d w pool user broker readerParallelism) quitV = do
    putStrLn $ "readerd starting (vaultaire v" ++ VERSION ++ ")"

    msgV <- newEmptyMVar
    contentsV <- newEmptyMVar

    -- Lock for Ceph connections to avoid race condition in nspr
    -- (https://github.com/ceph/ceph/pull/1424)
    radosLockV <- newMVar True

    -- Responses from workers
    outC <- newChan
    contentsC <- newChan

    telC <- newChan

    dV <- newMVar Map.empty

    let u = Mutexes {
        inbound = msgV,
        outbound = outC,
        telemetry = telC,
        directory = dV,
        contentsIn = contentsV,
        contentsOut = contentsC,
        radosLock = radosLockV
    }

    -- Startup reader threads
    replicateM_ w $
        linkThread $ reader (S.pack pool) (S.pack user) readerParallelism u

    linkThread $ contentsReader (S.pack pool) (S.pack user) u

    -- Startup communications threads
    linkThread $ receiver broker u d

    -- Block until end
    takeMVar quitV

  where
    linkThread a = Async.async a >>= Async.link


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
    <*> option
            (long "parallel-reads" <>
             short 'r' <>
             metavar "PARALLELREADS" <>
             value num <>
             showDefault <>
             help "Number of reads to execute simultaneously")
  where
    num = unsafePerformIO $ getNumCapabilities


readerCommandLineParser :: ParserInfo Options
readerCommandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Process to handle requests for data points from the vault" <>
                header "A data vault for metrics")

