{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE FunctionalDependencies   #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ScopedTypeVariables      #-}
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

-- | IO interactions for Marquise
module Marquise.IO
(
    MarquiseClientMonad(..),
    MarquiseServerMonad(..),
    BurstPath(..)
) where

import Control.Applicative ((<$>), (<*>))
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race)
import Control.Exception (ErrorCall, try)
import Control.Monad (unless, when)
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
import Data.Word (Word64)
import Marquise.Types (NameSpace (..))
import System.Directory (doesFileExist)
import System.IO (hClose)
import System.Posix.Files (removeLink, rename)
import System.Posix.Temp (mkstemp)
import System.ZMQ4 (Dealer (..), connect, receiveMulti, sendMulti,
                    withContext, withSocket)
import Vaultaire.CoreTypes (Address (..), Origin (..), isAddressExtended)

newtype BurstPath = BurstPath { unBurstPath :: FilePath }
    deriving (Show, Eq)

-- | This class is for convenience of testing. It encapsulates all IO
-- interaction that the client and server will do.
class Monad m => MarquiseClientMonad m where
    -- | This append does not imply that the given data is synced to disk, just
    -- that it is queued to do so. This assumes no state, so any file handles
    -- must be stashed globally or re-opened and closed.
    append :: NameSpace -> LB.ByteString -> m ()
    -- | Close any open handles and flush all previously appended datum to disk
    close :: NameSpace -> m ()

class MarquiseClientMonad m => MarquiseServerMonad m bp | m -> bp where
    -- | Atomically empty the underlying store and retrieve the next "burst" of
    -- appended datums. Appended datums are *not* separated. They're all
    -- concatenated together into the same ByteString.
    nextBurst :: NameSpace -> m (Maybe (bp, ByteString))
    -- | Clean up a sent burst. This should be called on a successfull ack.
    flagSent :: bp -> m ()

    -- | Send bytes upstream, returns when ack recieved.
    transmitBytes :: String      -- |^ Broker address
                  -> Origin      -- |^ Origin
                  -> ByteString  -- |^ Bytes to send
                  -> m ()

-- | "Dumb" IO implementation.
--
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

sendViaZMQ :: String -> Origin -> ByteString -> IO ()
sendViaZMQ broker (Origin origin) bytes =
    withContext $ \ctx ->
    withSocket ctx Dealer $ \s -> do
        connect s broker
        transmitLoop ["\x01"] s
  where
    -- We keep around all identifiers we've sent so that if we end up getting an
    -- ack for an earlier message we can be done earlier.
    transmitLoop identifiers s = do
        print "transmit loop"
        let identifier = head identifiers
        sendMulti s $ fromList [identifier, origin, bytes]

        -- We race, as I don't trust a foreign call to be interruptable.
        result <- race waitTimeout (receiveMulti s)
        case result of
            Left () ->
                let new_identifier = BS.append identifier identifier
                in transmitLoop (new_identifier:identifiers) s
            Right ack -> do
                print "got ack"
                case ack of
                    [identifier', empty] -> do
                        unless (identifier' `elem` identifiers) $
                            error "sendViaZMQ: panic: unknown identifier"
                        unless (BS.null empty) $
                            error $ "sendViaZMQ: panic: upstream: "
                                  ++ BS.unpack empty
                        return ()
                    _ ->
                        error "sendViaZMQ: panic: Invalid ack"


waitTimeout :: IO ()
waitTimeout = threadDelay 60000000

doSwap :: NameSpace -> IO (Maybe (BurstPath, ByteString))
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
                        error "verifySplit: corrupt data (extended burst)"
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

tmpTemplate :: NameSpace -> String
tmpTemplate ns = dataFilePath ns ++ "_"

dataFilePath :: NameSpace -> String
dataFilePath (NameSpace ns) = spoolDir ++ ns

-- Trailing slash is important
spoolDir :: FilePath
spoolDir = "/var/spool/marquise/"
