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
    readerCommandLineParser
)
where

import Codec.Compression.LZ4
import Control.Applicative
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad
import "mtl" Control.Monad.Error ()
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import qualified Data.Map.Strict as Map
import Data.Time.Clock
import GHC.Conc
import Network.BSD (getHostName)
import Options.Applicative hiding (reader)
import System.Environment (getProgName)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)
import qualified System.Rados as Rados
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
    argBrokerHost     :: !String
}

data Reply = Reply {
    envelope :: !ByteString, -- handled for us by the ROUTER socket, opaque.
    client   :: !ByteString, -- handled for us by the ROUTER socket, opaque.
    response :: !ByteString
}

data Mutexes = Mutexes {
    inbound   :: !(MVar [ByteString]),
    outbound  :: !(Chan Reply),
    telemetry :: !(Chan (String,String,String)),
    directory :: !(MVar Directory),
    contentsIn :: !(MVar [ByteString]),
    contentsOut :: !(Chan Reply)
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
    -> Mutexes
    -> IO ()
reader pool' user' Mutexes{..} =
    Rados.runConnect (Just user') (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool' $ forever $ do

            [envelope', client', origin', request'] <- liftIO $ takeMVar inbound
            a1 <- liftIO $ getCurrentTime

            case parseRequestMessage (Origin origin') request' of
                Left err -> do
                    output telemetry "error" (show err) ""
                Right qs -> do
                    output telemetry "request" (printf "%5d" (length qs)) "ranges"

                    forM_ qs $ \q -> do

                        let o  = requestOrigin q
                        let s  = requestSource q
                        let t1 = requestAlpha q
                        let t2 = requestOmega q

                        let is = Bucket.calculateTimemarks t1 t2

                        forM_ is $ \i -> do
                            m <- Bucket.readVaultObject o s i

                            unless (Map.null m) $ do
                                let ps = Bucket.pointsInRange t1 t2 m

                                let y' = encodePoints ps

                                let message' = case compress y' of
                                                Just b' -> b'
                                                Nothing -> S.empty

                                liftIO $ writeChan outbound (Reply envelope' client' message')

                        a2 <- liftIO $ getCurrentTime
                        let delta = diffUTCTime a2 a1
                        let deltaFloat = (fromRational $ toRational delta) :: Float
                        let deltaPadded = printf "%9.3f" deltaFloat
                        output telemetry "duration" deltaPadded "s"


            liftIO $ writeChan outbound (Reply envelope' client' S.empty)


contentsReader :: ByteString -> ByteString -> Mutexes -> IO ()
contentsReader pool user Mutexes{..} = do
    Rados.runConnect (Just user) (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool $ forever $ do
            [envelope, client, _, request] <- liftIO $ takeMVar contentsIn
            d <- liftIO $ takeMVar directory
            let origin = Origin request
            let lbl = Contents.formObjectLabel origin
            st <- Contents.readVaultObject lbl
            let d' = insertIntoDirectory d origin st
            liftIO $ putMVar directory d'
            let flatten l m = (Map.keys m) ++ l
            let sources = map createSourceResponse (Map.foldl flatten [] d')
            let burst = encodeSourceResponseBurst (createSourceResponseBurst sources)
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
            when d $ liftIO $ putStrLn $ printf "%-10s %-9s %s" (k ++ ":") v u
            let reply = [identifier', hostname', S.pack k, S.pack v, S.pack u]
            Zero.sendMulti tele (fromList reply)
--
-- inbound work
--

        linkThread . forever $ do
            msg <- Zero.receiveMulti router
            liftIO $ putMVar inbound msg

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
readerProgram (Options d w pool user broker) quitV = do
    putStrLn $ "readerd starting (vaultaire v" ++ VERSION ++ ")"

    msgV <- newEmptyMVar
    contentsV <- newEmptyMVar

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
        contentsOut = contentsC
    }

    -- Startup reader threads
    replicateM_ w $
        linkThread $ reader (S.pack pool) (S.pack user) u

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
  where
    num = unsafePerformIO $ getNumCapabilities


readerCommandLineParser :: ParserInfo Options
readerCommandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Process to handle requests for data points from the vault" <>
                header "A data vault for metrics")

