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
{-# LANGUAGE CPP                #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PackageImports     #-}
{-# LANGUAGE RecordWildCards    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module BufferDaemon where

import Blaze.ByteString.Builder
import Codec.Compression.LZ4
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.MVar
import Control.Concurrent.STM.TChan
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
import Data.UUID
import Data.UUID.V4
import Data.Time.Clock
import GHC.Conc
import Codec.Compression.LZ4
import Network.BSD (getHostName)
import Options.Applicative
import System.Environment (getProgName)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)
import System.Rados.Monadic (Pool)
import qualified System.Rados.Monadic as Rados
import System.ZMQ4.Monadic (runZMQ)
import qualified System.ZMQ4.Monadic as Zero
import Text.Printf
import Vaultaire.CommunicationsThread
import Data.Maybe
import Vaultaire.JournalFile

#include "../config.h"

data Options = Options {
    debug      :: !Bool,
    workers    :: !Int,
    time_limit :: !Int,
    byte_limit :: !Int,
    pool       :: !String,
    user       :: !String,
    broker     :: !String
}

-- This is how the broker will know to route all the way back to the client,
-- and how the client will know which message we are referencing.
data Ident = Ident {
    envelope  :: !ByteString, -- handled for us by the router socket, opaque.
    client    :: !ByteString, -- handled for us by the router socket, opaque.
    messageID :: !ByteString  -- a uint16_t, but opaque to us.
}

data Ack = Ack {
    ident   :: Ident,
    failure :: !ByteString -- possible failure, empty for success
}

-- Entrypoint
program :: Options -> MVar () -> IO ()
program Options{..} quit_mvar = do
    putStrLn $ "bufferd starting (vaultaire v"  ++ VERSION ++ ")"

    in_chan <- newTChanIO
    telemetry_chan <- newTChanIO
    ack_chan <- newTChanIO

    linkThread $ telemetry_sender broker debug telemetry_chan
    linkThread $ receiver broker in_chan ack_chan time_limit byte_limit

    replicateM_ workers $
        linkThread $ worker (S.pack pool)
                            (S.pack user)
                            in_chan
                            ack_chan
                            telemetry_chan

    takeMVar quit_mvar
    putStrLn "bufferd stopping"
  where
    linkThread a = Async.async a >>= Async.link

worker
    :: ByteString
    -> ByteString
    -> TChan (ByteString, Ack)
    -> TChan Ack
    -> TChan Telemetry
    -> IO ()
worker pool user in_chan ack_chan telemtry_chan = do
    counter <- newMVar 0
    nonce <- toString <$> nextRandom
    let journal_file_name = "01_journal"

    Rados.runConnect (Just user) (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool $ forever $ do
            let readIncoming = liftIO $ atomically $ readWholeChan in_chan
            writes_pending <- decompressMessage <$> readIncoming
            output telemtry_chan
                   "writing" (show $ length writes_pending) "journal blocks"

            -- Write all the blocks first so that we know they're safe.
            writes <- forM writes_pending $ \(payload,ack) -> do
                name <- objectName nonce counter
                async <- Rados.runAsync . Rados.runObject name $
                    Rados.writeFull payload
                return (async, (name, S.length payload), ack)

            -- Only want to ack successful writes
            non_failed <- catMaybes <$> forM writes checkFailure
                       -- Now we can write the journal index and ack
            let !journal_contents = makeInboundJournal $ map fst non_failed
            journal_write <- Rados.runObject journal_file_name $
                Rados.append journal_contents

            case journal_write of
                Just e -> liftIO $ putStrLn $
                            "Error writing journal:" ++ show e
                Nothing -> return ()
  where
    checkFailure (async, entry, ack) = do
            result <- Rados.waitSafe async
            case result of
                Nothing -> return $ Just (entry, ack)
                Just e -> do
                    liftIO $ putStrLn $ "Error writing block: " ++ show e
                    return Nothing


    decompressMessage = map (\(msg, ack) -> (fromJust (decompress msg), ack))
    objectName nonce counter = liftIO $ do
        n <- takeMVar counter 
        putMVar counter (n + 1)
        return $ S.pack $ "01_" ++ nonce ++ "_" ++ show n

readWholeChan :: TChan a -> STM [a]
readWholeChan ch = do
    next <- tryReadTChan ch
    case next of
        Nothing -> return []
        Just x  -> (x:) <$> readWholeChan ch


receiver :: String -> TChan (ByteString, Ack) -> TChan Ack -> Int -> Int -> IO ()
receiver broker in_chan ack_chan time_limit byte_limit = runZMQ $ do
    work <- Zero.socket Zero.Router
    Zero.setReceiveHighWM (Zero.restrict 0) work
    Zero.connect work ("tcp://" ++ broker ++ ":5561")

    loop mempty 0
  where
    loop acc bytes = do
        res <- Zero.poll 100 [Zero.Sock work [Zero.In] Nothing]
        case res of
            -- Message waiting
            [[Zero.In]] -> sendWork work in_chan
            -- Timeout, do nothing.
            [[]]        -> return ()
            _           -> error "reciever: unpossible"

        -- Between each timeout or recieved message, send all outstanding
        -- acks.
        sendAcks work ack_chan
    sendWork sock ch = do
        [envelope, client, identifier, message] <- Zero.receiveMulti sock
        let ident = Ident envelope client identifier
        let ack = Ack ident ""
        liftIO $ atomically $ writeTChan ch (message, ack)
        

    sendAcks sock chan = do
        next <- liftIO . atomically $ tryReadTChan chan
        case next of
            Nothing -> return ()
            Just (Ack Ident{..} failure) -> do
                let reply = fromList [envelope, client, messageID, failure]
                Zero.sendMulti sock reply
                sendAcks sock chan


-- Handle command line arguments.
commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Ingestion worker to feed points into the vault" <>
                header "A data vault for metrics")

toplevel :: Parser Options
toplevel = Options
    <$> switch
            (long "debug" <>
             short 'd' <>
             help "Write debug telemetry to stdout")
    <*> option
            (long "workers" <>
             short 'w' <>
             metavar "NUM" <>
             value num <>
             showDefault <>
             help "Number of bursts to process concurrently")
    <*> option
            (long "connections" <>
             short 'c' <>
             metavar "NUM" <>
             value 1 <>
             showDefault <>
             help "Number of connections writing to Ceph concurrently")
    <*> option
            (long "timelimit" <>
             short 's' <>
             value 10 <>
             metavar "NUM" <>
             showDefault <>
             help "Maximum time to wait before flushing buffers to Ceph")
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
