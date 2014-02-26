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
{-# OPTIONS -fno-warn-unused-imports #-}
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
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock
import GHC.Conc
import Options.Applicative hiding (reader)
import System.Environment (getArgs, getProgName)
import System.IO.Unsafe (unsafePerformIO)
import System.Rados (Pool)
import qualified System.Rados as Rados
import System.ZMQ4.Monadic (Pub, Router)
import qualified System.ZMQ4.Monadic as Zero
import Text.Printf

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents

data Options = Options {
    optGlobalDebug    :: !Bool,
    optGlobalWorkers  :: !Int,
    argGlobalPoolName :: !String,
    optGlobalUserName :: !String,
    argBrokerHost     :: !String
}

data Reply = Reply {
    envelope'   :: !ByteString, -- handled for us by the ROUTER socket, opaque.
    originator' :: !ByteString, -- handled for us by the ROUTER socket, opaque.
    response'   :: !ByteString
}


data Mutexes = Mutexes {
    inbound   :: !(MVar [ByteString]),
    outbound  :: !(Chan Reply),
    telemetry :: !(Chan (String,String,String)),
    directory :: !(MVar Directory)
}


-- FIXME time range!
findPoints :: Request -> Pool [Point]
findPoints (Request o s t1 _) = do
    m <- Bucket.readVaultObject o s t1
    return $ Map.elems m


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

            [envelope', originator', origin', request'] <- liftIO $ takeMVar inbound
            t1 <- liftIO $ getCurrentTime

            case parseRequestMessage (Origin origin') request' of
                Left err -> do
                    -- temporary, replace with telemetry
                    output telemetry "error" (show err) ""
                Right qs -> do

                    forM_ qs $ \q -> do
                        ps <- findPoints q
                        let y' = encodePoints ps

                        let message' = case compress y' of
                                            Just b' -> b'
                                            Nothing -> S.empty

                        t2 <- liftIO $ getCurrentTime
                        output telemetry "duration" (show $ diffUTCTime t2 t1) "s"

                        liftIO $ writeChan outbound (Reply envelope' originator' message')

            liftIO $ writeChan outbound (Reply envelope' originator' S.empty)



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
        Zero.bind tele "tcp://*:5569"

--
-- telemetry
--

        linkThread . forever $ do
            (k,v,u) <- liftIO $ readChan telemetry
            when d $ liftIO $ putStrLn $ printf "%-10s %-9s %s" (k ++ ":") v u
            let reply = [S.pack k, S.pack v, S.pack u]
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
            let reply = [envelope', originator', response']
            Zero.sendMulti router (fromList reply)

  where

    linkThread a = Zero.async a >>= liftIO . Async.link


readerProgram :: Options -> MVar () -> IO ()
readerProgram (Options d w pool user broker) quitV = do
    msgV <- newEmptyMVar

    -- Responses from workers
    outC <- newChan

    telC <- newChan

    dV <- newMVar Map.empty

    let u = Mutexes {
        inbound = msgV,
        outbound = outC,
        telemetry = telC,
        directory = dV
    }

    -- Startup reader threads
    replicateM_ w $
        linkThread $ reader (S.pack pool) (S.pack user) u


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

