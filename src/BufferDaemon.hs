-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module BufferDaemon where

import Codec.Compression.LZ4
import Control.Arrow ((***))
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TBChan
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.List
import Data.List.NonEmpty (fromList)
import Data.Maybe
import Data.Serialize
import Data.Time.Clock
import Data.UUID (toString)
import Data.UUID.V4
import GHC.Conc (getNumCapabilities)
import Options.Applicative
import System.IO.Unsafe (unsafePerformIO)
import qualified System.Rados.Monadic as Rados
import System.ZMQ4.Monadic (runZMQ)
import qualified System.ZMQ4.Monadic as Zero
import Vaultaire.CommunicationsThread
import Vaultaire.JournalFile

#include "../config.h"

data Options = Options {
    debug     :: !Bool,
    workers   :: !Int,
    timeLimit :: !Int,
    byteLimit :: !Int,
    pool      :: !String,
    user      :: !String,
    broker    :: !String
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

type BuiltBlock = (ByteString, Int, [Ack])
type BuildFailure = Ack

-- Entrypoint
program :: Options -> MVar () -> IO ()
program Options{..} quit_mvar = do
    putStrLn $ "bufferd starting (vaultaire v"  ++ VERSION ++ ")"

    in_chan <- newTBChanIO 64
    telemetry_chan <- newTChanIO
    ack_chan <- newTBChanIO 64

    linkThread $ telemetry_sender broker debug telemetry_chan
    linkThread $ receiver broker in_chan ack_chan timeLimit byteLimit

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
    -> TBChan [(ByteString, Ident)]
    -> TBChan Ack
    -> TChan Telemetry
    -> IO ()
worker pool user in_chan ack_chan telemetry_chan = do
    counter <- newMVar (0 :: Integer)
    -- Seesed in a system dependent fashion.
    nonce <- toString <$> nextRandom
    let journal_file_name = "01_journal"

    Rados.runConnect (Just user) (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool $ forever $ do
            let readIncoming = liftIO $ atomically $ readWholeChan in_chan

            (writes_pending, parse_failures) <- buildBlocks <$> readIncoming

            sendAcks ack_chan parse_failures

            unless (null parse_failures) $
                output telemetry_chan "error"
                                      "failed to decompress message(s)"
                                      ""

            output telemetry_chan "writing"
                                  (show $ length writes_pending)
                                  "blocks"

            -- Write all the blocks first so that we know they're safe.
            writes <- forM writes_pending $ \(payload,len,ack) -> do
                name <- objectName nonce counter
                async <- Rados.runAsync . Rados.runObject name $
                    Rados.writeFull payload
                -- (name, len) are an single entry in the journal index
                return (async, (name, len), ack)

            -- Only want to ack successful writes
            non_failed <- catMaybes <$> forM writes checkFailure

            -- Now we can write the journal index and ack
            let !journal_contents = makeInboundJournal $ map fst non_failed
            journal_write <- Rados.runObject journal_file_name $
                Rados.append journal_contents

            case journal_write of
                Just e -> liftIO $ putStrLn $
                            "Error writing journal:" ++ show e
                Nothing ->
                    let acks = concatMap snd non_failed in
                        sendAcks ack_chan acks
  where
    checkFailure (async, journal_entry, ack) = do
            result <- Rados.waitSafe async
            case result of
                Nothing -> return $ Just (journal_entry, ack)
                Just e -> do
                    liftIO $ putStrLn $ "Error writing block: " ++ show e
                    return Nothing


    buildBlocks :: [[(ByteString, Ident)]] -> ([BuiltBlock], [BuildFailure])
    buildBlocks = foldl' combine ([], []) . map buildBlock
      where
        combine :: ([BuiltBlock], [BuildFailure])
                -> (BuiltBlock, [BuildFailure])
                -> ([BuiltBlock], [BuildFailure])
        combine (blocks, failures) = (:blocks) *** (failures++)

        buildBlock :: [(ByteString, Ident)]
                   -> (BuiltBlock, [BuildFailure])
        buildBlock xs =
            let (bursts, acks, fails) = foldl' tryDecompress ([],[],[]) xs
                serialized  = serialize bursts
                compressed  = mustCompress serialized
                built_block = (compressed, S.length serialized, acks)
            in (built_block, fails)
          where
            tryDecompress (msgs, acks, fails) (bs, ident) =
                case decompress bs of
                    Just msg -> ( msg:msgs
                                , Ack ident "":acks
                                , fails )
                    Nothing ->  ( msgs
                                , acks
                                , Ack ident "failed to decompress":fails )
            serialize = runPut . put
            mustCompress = fromJust . compress

    objectName nonce counter = liftIO $ do
        n <- takeMVar counter
        putMVar counter (n + 1)
        return $ S.pack $ "01_" ++ nonce ++ "_" ++ show n

    sendAcks ch =
        mapM_ (liftIO . atomically . writeTBChan ch)

-- Block on first element of chan, if that exists, try to read more.
readWholeChan :: TBChan a -> STM [a]
readWholeChan ch = do
    x <- readTBChan ch
    xs <- readTail
    return (x:xs)
  where
    readTail = do
        next <- tryReadTBChan ch
        case next of
            Nothing -> return []
            Just x  -> (x:) <$> readTail


receiver :: String -> TBChan [(ByteString, Ident)] -> TBChan Ack -> Int -> Int -> IO ()
receiver broker in_chan ack_chan time_limit byte_limit = runZMQ $ do
    work <- Zero.socket Zero.Router
    Zero.setReceiveHighWM (Zero.restrict (0 :: Int)) work
    Zero.connect work ("tcp://" ++ broker ++ ":5561")

    liftIO getCurrentTime >>= loop work [] 0
  where
    loop sock acc bytes last_sent = do
        res <- Zero.poll 100 [Zero.Sock sock [Zero.In] Nothing]
        msg <- case res of
            -- Message waiting
            [[Zero.In]] -> Zero.receiveMulti sock >>= prepareMessage
            -- Timeout, do nothing.
            [[]]        -> return Nothing
            _           -> error "reciever: unpossible"

        -- Between each timeout or recieved message, send all outstanding
        -- acks.
        sendAcks sock ack_chan

        let msg_size = maybe 0 (S.length . fst) msg
        now <- liftIO getCurrentTime
        let expiry = now `diffUTCTime` last_sent > fromIntegral time_limit

        when (bytes + msg_size > byte_limit || expiry ) $ do
            case msg of
                Nothing -> sendWork acc     in_chan
                Just m  -> sendWork (m:acc) in_chan
            loop sock [] 0 now

        case msg of
            Nothing -> loop sock acc bytes last_sent
            Just m  -> loop sock (m:acc) (bytes + msg_size) last_sent

    prepareMessage [envelope, client, identifier, message] =
      let ident = Ident envelope client identifier
      in return $ Just (message, ident)
    prepareMessage _ = do
        liftIO $ putStrLn "Invalid ZMQ message recieved"
        return Nothing

    sendWork [] _ = return ()
    sendWork msg ch =
        liftIO $ atomically $ writeTBChan ch msg

    sendAcks sock chan = do
        next <- liftIO $ atomically $ tryReadTBChan chan
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
                progDesc "Buffer daemon to write points to journal" <>
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
            (long "timelimit" <>
             short 's' <>
             value 10 <>
             metavar "NUM" <>
             showDefault <>
             help "Number of seconds to wait before flushing")
    <*> option
            (long "bytelimit" <>
             short 'b' <>
             metavar "NUM" <>
             value 4194304 <>
             showDefault <>
             help "Number of bytes to store before flushing")
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
    num = unsafePerformIO getNumCapabilities
