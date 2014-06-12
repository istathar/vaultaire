{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE ForeignFunctionInterface   #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | IO interactions for Marquise
module Marquise.IO
(
    MarquiseClientMonad(..),
    MarquiseServerMonad(..),
    BurstPath(..),
    ContentsClientMonad(..),
    ContentsConfig(..),
    spoolDir,
    -- * Errors
    VaultaireTimeout
) where

import Control.Applicative ((<$>), (<*>))
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, link, race)
import Control.Exception (ErrorCall, Exception, SomeException (..), try)
import Control.Monad (unless, when)
import Control.Monad.Reader (MonadReader, ReaderT, ask)
import Control.Monad.State (evalStateT, get, lift, put)
import Data.Attoparsec (Parser)
import qualified Data.Attoparsec as Parser
import Data.Attoparsec.ByteString.Lazy (maybeResult, parse)
import Data.Attoparsec.Combinator (eitherP, many')
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as LB
import Data.List.NonEmpty (fromList)
import Data.Maybe (fromMaybe)
import Data.Packer (getWord64LE, runUnpacking, unpackSkip)
import Data.Typeable (Typeable)
import Data.Word (Word64)
import Marquise.Types (SpoolName (..))
import Pipes (Producer, liftIO, runEffect, yield, (>->))
import Pipes.Concurrent (Buffer (..), atomically, fromInput, spawn', toOutput)
import System.Directory (doesFileExist)
import System.IO (hClose)
import System.Posix.Files (removeLink, rename)
import System.Posix.Temp (mkstemp)
import System.ZMQ4 (Dealer (..), Socket, connect, receiveMulti, sendMulti,
                    withContext, withSocket)
import Vaultaire.Types

newtype BurstPath = BurstPath { unBurstPath :: FilePath }
    deriving (Show, Eq)

data VaultaireTimeout = VaultaireTimeout
    deriving (Typeable, Show)
instance Exception VaultaireTimeout

data ContentsConfig = ContentsConfig String Origin

-- | This class is for convenience of testing. It encapsulates all IO
-- interaction that the client and server will do.
class Monad m => MarquiseClientMonad m where
    -- | This append does not imply that the given data is synced to disk, just
    -- that it is queued to do so. This assumes no state, so any file handles
    -- must be stashed globally or re-opened and closed.
    append :: SpoolName -> LB.ByteString -> m ()
    -- | Close any open handles and flush all previously appended datum to disk
    close :: SpoolName -> m ()

class MarquiseClientMonad m => MarquiseServerMonad m bp | m -> bp where
    -- | Atomically empty the underlying store and retrieve the next "burst" of
    -- appended datums. Appended datums are *not* separated. They're all
    -- concatenated together into the same ByteString.
    nextBurst :: SpoolName -> m (Maybe (bp, ByteString))
    -- | Clean up a sent burst. This should be called on a successfull ack.
    flagSent :: bp -> m ()

    -- | Send bytes upstream, returns when ack recieved.
    transmitBytes :: String      -- ^ Broker address
                  -> Origin      -- ^ Origin
                  -> ByteString  -- ^ Bytes to send
                  -> m ()


class MonadReader ContentsConfig m => ContentsClientMonad m where
    requestUniqueAddress :: m (Either SomeException Address)
    requestSourceDictUpdate :: Address -> SourceDict -> m (Either SomeException ())
    requestSourceDictRemoval :: Address -> SourceDict -> m (Either SomeException ())
    requestList :: Producer (Address, SourceDict) m ()

instance ContentsClientMonad (ReaderT ContentsConfig IO) where
    requestUniqueAddress = do
        response <- singleRequest GenerateNewAddress
        return $ case response of
            Right (RandomAddress addr) ->
                Right addr
            Right _ -> error "requestUniqueAddress: Invalid response"
            Left e -> Left e

    requestSourceDictUpdate addr source_dict = do
        response <- singleRequest $ UpdateSourceTag addr source_dict
        return $ case response of
            Right UpdateSuccess -> Right ()
            Right _ -> error "requestSourceDictUpdate: Invalid response"
            Left e -> Left e

    requestSourceDictRemoval addr source_dict = do
        response <- singleRequest $ RemoveSourceTag addr source_dict
        return $ case response of
            Right RemoveSuccess -> Right ()
            Right _ -> error "requestSourceDictRemoval: Invalid response"
            Left e -> Left e

    requestList = do
        ContentsConfig broker origin <- lift ask
        (output, input, seal) <- liftIO $ spawn' Single

        thread <- liftIO . async . withContentsSocket broker $ \s -> do
            sendRequest "" origin s ContentsListRequest
            runEffect (loop s >-> toOutput output)
            liftIO $ atomically seal

        liftIO $ link thread
        fromInput input
      where
        loop socket = do
            resp <- liftIO $ waitResponse socket ""
            case resp of
                Left e -> error $ show e
                Right (ContentsListEntry addr dict) -> do
                    yield (addr, dict)
                    loop socket
                Right EndOfContentsList ->
                    return ()
                Right _ ->
                    error "requestList loop: Invalid response"

singleRequest :: (WireFormat response, WireFormat request)
              => request
              -> ReaderT ContentsConfig IO (Either SomeException response)
singleRequest req = contentsRequest (requestResponse req "")

contentsRequest :: (Origin -> Socket Dealer -> IO a)
                -> ReaderT ContentsConfig IO a
contentsRequest f = do
    ContentsConfig broker origin <- ask
    lift $ withContentsSocket broker (f origin)

withContentsSocket :: String -> (Socket Dealer -> IO a) -> IO a
withContentsSocket broker = withVaultaireSocket ("tcp://" ++ broker ++ ":5580")

-- Making MonadIO m an instance is impractical, as it would require
-- undecidable instances.
--
-- This could be more efficient if the handle were kept in a "global
-- variable", using the noinline IORef hack.
instance MarquiseClientMonad IO where
    append ns = LB.appendFile (dataFilePath ns)

    close _ = c_sync

instance MarquiseServerMonad IO BurstPath where
    nextBurst ns = do
        exists <- doesFileExist (dataFilePath ns)
        if exists
            then doSwap ns
            else return Nothing

    flagSent = removeLink . unBurstPath

    transmitBytes = sendViaZMQ

-- | Send the request over the socket, waiting for a response, with a timeout.
-- This can be used on non-idempotent actions, as it will only request once.
--
-- This is not thread-safe and should be the sole user of the connection.
requestResponse :: (WireFormat request, WireFormat response)
              => request
              -> ByteString
              -> Origin
              -> Socket Dealer
              -> IO (Either SomeException response)
requestResponse request identifier origin sock = do
    sendRequest identifier origin sock request
    waitResponse sock identifier

waitResponse :: WireFormat response
              => Socket Dealer
              -> ByteString
              -> IO (Either SomeException response)
waitResponse sock identifier = either Left id <$> race waitTimeout doWait
  where
    doWait = do
        resp <- receiveMulti sock
        case resp of
            [identifier', msg] ->
                if identifier' == identifier
                    then return $ fromWire msg
                    else waitResponse sock identifier
            _ -> return $ Left $ SomeException $ userError "not two msg parts"

sendRequest :: WireFormat request => ByteString -> Origin -> Socket Dealer -> request -> IO ()
sendRequest identifier (Origin origin) sock request =
    let payload = fromList [identifier, origin, toWire request]
    in sendMulti sock payload

sendViaZMQ :: String -> Origin -> ByteString -> IO ()
sendViaZMQ broker (Origin origin) bytes =
    withVaultaireSocket ("tcp://" ++ broker ++ ":5560") (transmitLoop ["\x01"])
  where
    -- We keep around all identifiers we've sent so that if we end up getting an
    -- ack for an earlier message we can be done earlier.
    transmitLoop identifiers s = do
        let identifier = head identifiers
        sendMulti s $ fromList [identifier, origin, bytes]

        -- We race, as I don't trust a foreign call to be interruptable.
        result <- race waitTimeout (receiveMulti s)
        case result of
            Left _ ->
                let new_identifier = BS.append identifier identifier
                in transmitLoop (new_identifier:identifiers) s
            Right ack ->
                case ack of
                    [identifier', payload] -> do
                        unless (identifier' `elem` identifiers) $
                            error "sendViaZMQ: unknown identifier"
                        case fromWire payload of
                            Left e ->
                                error $ "sendViaZMQ: parse: " ++ show e
                            Right OnDisk ->
                                return ()
                            Right InvalidWriteOrigin ->
                                error "sendViaZMQ: Invalid origin"
                    _ ->
                        error "sendViaZMQ: Invalid response"

withVaultaireSocket :: String -> (Socket Dealer -> IO a) -> IO a
withVaultaireSocket broker f =
    withContext $ \ctx ->
    withSocket ctx Dealer $ \s -> do
        connect s broker
        f s

waitTimeout :: IO SomeException
waitTimeout = do
    threadDelay 60000000
    return $ SomeException VaultaireTimeout

doSwap :: SpoolName -> IO (Maybe (BurstPath, ByteString))
doSwap ns =  do
    -- Create a temp file to atomically move our data into.
    (tmp_path, tmp_handle) <- mkstemp (tmpTemplate ns)
    hClose tmp_handle
    rename (dataFilePath ns) tmp_path

    -- Read the whole file lazily
    parsed <- try $ verifySplit <$> LB.readFile tmp_path
    case parsed of
        Right (burst_data, remainder) -> do
            -- If the file is huge, we want to put the remainder back, this
            -- happens lazily. Larger than memory files should not be an
            -- issue.
            unless (LB.null remainder) $ append ns remainder
            return $ Just (BurstPath tmp_path, burst_data)
        Left (_ :: ErrorCall) ->
            error $ "nextBurst: panic: corrupt data:" ++ tmp_path

-- | Verify that the data is valid, we have to do this verification to split at
-- a valid boundary anyway.
verifySplit :: LB.ByteString -> (ByteString, LB.ByteString)
verifySplit = fromMaybe (error "verifySplit: impossible due to many'")
                        . maybeResult . parse verify
  where
    verify = (,) <$> (LB.toStrict . LB.fromChunks <$> chunks)
                 <*> Parser.takeLazyByteString
    -- Yes, a linked list of bytestrings isn't the most efficient structure.
    -- It's more than fast enough.
    chunks :: Parser [ByteString]
    chunks = flip evalStateT 0 $ many' $ do
        current_size <- get
        when (current_size > idealBurstSize) (lift $ fail "I am full now.")

        packet <- lift $ Parser.take 24

        case extendedSize packet of
            Just len -> do
                -- Mast ensure that we get this many bytes now, or attoparsec
                -- will just backtrack on us. We do this with a dummy parser
                -- inside an eitherP
                extended <- lift $ eitherP (Parser.take len) (return ())
                case extended of
                    Left bytes -> do
                        put (current_size + fromIntegral len + 24)
                        return $ BS.append packet bytes
                    Right () ->
                        error $ "verifySplit: corrupt data (extended burst) at: "
                                ++ show current_size
            Nothing -> do
                put (current_size + 24)
                return packet

    extendedSize :: ByteString -> Maybe Int
    extendedSize packet = flip runUnpacking packet $ do
        addr <- Address <$> getWord64LE
        if isAddressExtended addr
            then do
                unpackSkip 8
                Just . fromIntegral <$> getWord64LE -- length
            else
                return Nothing

-- A burst should be, at maximum, very close to this side, unless the user
-- decides to send a very long extended point.
idealBurstSize :: Word64
idealBurstSize = 1048576 -- 1MB

foreign import ccall "unistd.h sync" c_sync :: IO ()

tmpTemplate :: SpoolName -> String
tmpTemplate ns = dataFilePath ns ++ "_"

dataFilePath :: SpoolName -> String
dataFilePath (SpoolName ns) = spoolDir ++ ns

-- Trailing slash is important
spoolDir :: FilePath
spoolDir = "/var/spool/marquise/"
