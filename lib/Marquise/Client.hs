--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | Marquise client interface for sending data to the vault.
--
-- This module provides functions for preparing and queuing points to be sent
-- by a Marquise server to the vault.
--
-- If you call close, you can be assured that your data is safe and will at
-- some point end up in the data vault (excluding local disk failure). This
-- assumption is based on a functional marquise daemon with connectivity
-- eventually running within your namespace.
--
-- We provide no way to *absolutely* ensure that a point is currently written
-- to the vault. Such a guarantee would require blocking and complex queuing,
-- or observing various underlying mechanisms that should ideally remain
-- abstract.
--

{-# LANGUAGE MultiParamTypeClasses #-}

module Marquise.Client
(
    -- | * Utility functions
    -- Note: You may read MarquiseSpoolFileMonad m as IO.
    hashIdentifier,
    makeSpoolName,

    -- | * Contents daemon requests
    withContentsConnection,
    requestUnique,
    makeSourceDict,
    updateSourceDict,
    removeSourceDict,
    enumerateOrigin,

    -- | * Queuing data to be sent to vaultaire
    createSpoolFiles,
    queueSimple,
    queueExtended,
    queueSourceDictUpdate,
    flush,

    -- | Reading from Vaultaire
    withReaderConnection,
    readExtended,
    readSimple,
    decodeExtended,
    decodeSimple,

    -- * Types
    SourceDict,
    SpoolName,
    SpoolFiles,
    Address,
    Origin(..),
    TimeStamp(..),
    SimpleBurst(..),
    SimplePoint(..),
) where

import Control.Applicative
import Control.Exception (SomeException (..), throw)
import Control.Monad.Reader
import Crypto.MAC.SipHash
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Char (isAlphaNum)
import Data.Packer
import Data.Word (Word64)
import Marquise.Classes
import Marquise.IO ()
import Marquise.Types
import Pipes
import Vaultaire.Types

-- | Create a SpoolName. Only alphanumeric characters are allowed, max length
-- is 32 characters.
makeSpoolName :: String -> Either SomeException SpoolName
makeSpoolName s
    | any (not . isAlphaNum) s = Left (SomeException InvalidSpoolName)
    | otherwise = Right (SpoolName s)

-- | Create a name in the spool. Only alphanumeric characters are allowed, max length
-- is 32 characters.
createSpoolFiles :: MarquiseSpoolFileMonad m
                 => String
                 -> m SpoolFiles
createSpoolFiles s =
    case makeSpoolName s of
        Left e -> throw e
        Right sn -> createDirectories sn >> randomSpoolFiles sn

-- | Deterministically convert a ByteString to an Address, this uses siphash.
hashIdentifier :: ByteString -> Address
hashIdentifier = Address . (`clearBit` 0) . unSipHash . hash iv
  where
    iv = SipKey 0 0
    unSipHash (SipHash h) = h :: Word64

-- | Generate an un-used Address. You will need to store this for later re-use.
requestUnique :: MarquiseContentsMonad m conn
               => Origin
               -> conn
               -> m (Either SomeException Address)
requestUnique origin conn =  do
    sendContentsRequest GenerateNewAddress origin conn
    response <- recvContentsResponse conn
    return $ case response of
        Right (RandomAddress addr) ->
            Right addr
        Right _ -> error "requestUnique: Invalid response"
        Left e -> Left e

-- | Set the key,value tags as metadata on the given Address.
updateSourceDict :: MarquiseContentsMonad m conn
                 => Address
                 -> SourceDict
                 -> Origin
                 -> conn
                 -> m (Either SomeException ())
updateSourceDict addr source_dict origin conn =  do
    sendContentsRequest (UpdateSourceTag addr source_dict) origin conn
    response <- recvContentsResponse conn
    return $ case response of
        Right UpdateSuccess -> Right ()
        Right _ -> error "requestSourceDictUpdate: Invalid response"
        Left e -> Left e

-- | Remove the supplied key,value tags from metadata on the Address, if present.
removeSourceDict :: MarquiseContentsMonad m conn
                 => Address
                 -> SourceDict
                 -> Origin
                 -> conn
                 -> m (Either SomeException ())
removeSourceDict addr source_dict origin conn = do
    sendContentsRequest (RemoveSourceTag addr source_dict) origin conn
    response <- recvContentsResponse conn
    return $ case response of
        Right RemoveSuccess -> Right ()
        Right _ -> error "requestSourceDictRemoval: Invalid response"
        Left e -> Left e

enumerateOrigin :: MarquiseContentsMonad m conn
                => Origin
                -> conn
                -> Producer (Address, SourceDict) m ()
enumerateOrigin origin conn = do
    lift $ sendContentsRequest ContentsListRequest origin conn
    loop
  where
    loop = do
        resp <- lift $ recvContentsResponse conn
        case resp of
            Left e -> error $ show e
            Right (ContentsListEntry addr dict) ->
                yield (addr, dict) >> loop
            Right EndOfContentsList ->
                return ()
            Right _ ->
                error "enumerateOrigin loop: Invalid response"

readSimple :: MarquiseReaderMonad m conn
           => Address
           -> Word64
           -> Word64
           -> Origin
           -> conn
           -> Producer SimpleBurst m ()
readSimple addr start end origin conn = do
    lift $ sendReaderRequest (SimpleReadRequest addr start end) origin conn
    loop
  where
    loop = do
        response <- lift $ recvReaderResponse conn
        case response of
            Right (SimpleStream burst) ->
                yield burst >> loop
            Right EndOfStream ->
                return ()
            Right InvalidReadOrigin ->
                error "readSimple loop: Invalid origin"
            Right _ ->
                error "readSimple loop: Invalid response"
            Left e ->
                throw e

readExtended :: MarquiseReaderMonad m conn
             => Address
             -> Word64
             -> Word64
             -> Origin
             -> conn
             -> Producer ExtendedBurst m ()
readExtended addr start end origin conn = do
    lift $ sendReaderRequest (ExtendedReadRequest addr start end) origin conn
    loop
  where
    loop = do
        response <- lift $ recvReaderResponse conn
        case response of
            Right (ExtendedStream burst) ->
                yield burst >> loop
            Right EndOfStream ->
                return ()
            Right _ ->
                error "readSimple loop: Invalid response"
            Left e ->
                throw e

decodeSimple :: Monad m => Pipe SimpleBurst SimplePoint m ()
decodeSimple = forever (unSimpleBurst <$> await >>= emitFrom 0)
  where
    emitFrom os chunk
        | os >= BS.length chunk = return ()
        | otherwise = do
            yield $ flip runUnpacking chunk $ do
                unpackSetPosition os
                addr <- Address <$> getWord64LE
                SimplePoint addr <$> getWord64LE <*> getWord64LE

            emitFrom (os + 24) chunk


decodeExtended :: Monad m => Pipe ExtendedBurst ExtendedPoint m ()
decodeExtended = forever (unExtendedBurst <$> await >>= emitFrom 0)
  where
    emitFrom os chunk
        | os >= BS.length chunk = return ()
        | otherwise = do
            let result = runUnpacking (unpack os) chunk
            yield result

            let size = BS.length (extendedPayload result) + 24
            emitFrom (os + size) chunk

    unpack os = do
        unpackSetPosition os
        addr <- Address <$> getWord64LE
        time <- getWord64LE
        len <- fromIntegral <$> getWord64LE
        payload <- if len == 0
                       then return BS.empty
                       else getBytes len

        return $ ExtendedPoint addr time payload

-- | Send a "simple" data point. Interpretation of this point, e.g.
-- float/signed is up to you, but it must be sent in the form of a Word64.
queueSimple
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Address
    -> TimeStamp
    -> Word64
    -> m ()
queueSimple sfs (Address ad) (TimeStamp ts) w = appendPoints sfs bytes
  where
    bytes = runPacking 24 $ do
        putWord64LE (ad `clearBit` 0)
        putWord64LE ts
        putWord64LE w

-- | Send an "extended" data point. Again, representation is up to you.
queueExtended
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Address
    -> TimeStamp
    -> ByteString
    -> m ()
queueExtended sfs (Address ad) (TimeStamp ts) bs = appendPoints sfs bytes
  where
    len = BS.length bs
    bytes = runPacking (24 + len) $ do
        putWord64LE (ad `setBit` 0)
        putWord64LE ts
        putWord64LE $ fromIntegral len
        putBytes bs

queueSourceDictUpdate
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> Address
    -> SourceDict
    -> m ()
queueSourceDictUpdate sfs (Address addr) source_dict = appendContents sfs bytes
  where
    source_dict_bytes = toWire source_dict
    source_dict_len = BS.length source_dict_bytes
    bytes = runPacking (source_dict_len + 16) $ do
        putWord64LE addr
        putWord64LE (fromIntegral source_dict_len)
        putBytes source_dict_bytes

-- | Ensure that all sent points have hit the local disk.
flush
    :: MarquiseSpoolFileMonad m
    => SpoolFiles
    -> m ()
flush = close

