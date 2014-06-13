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

{-# LANGUAGE GADTs #-}

module Marquise.Client
(
    -- | * Utility functions
    -- Note: You may read MarquiseSpoolFileMonad m as IO.
    makeSpoolName,
    hashIdentifier,

    -- | * Contents daemon requests
    withContentsConnection,
    requestUnique,
    makeSourceDict,
    updateSourceDict,
    removeSourceDict,
    enumerateOrigin,

    -- | * Sending data to Vaultaire
    sendSimple,
    sendExtended,
    flush,

    -- * Types
    SpoolName,
    Address,
    TimeStamp(..),
) where

import Control.Exception (SomeException)
import Control.Monad.Reader
import Crypto.MAC.SipHash
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import Data.Char (isAlphaNum)
import Data.Packer (putBytes, putWord64LE, runPacking)
import Data.Word (Word64)
import Marquise.Classes
import Marquise.IO ()
import Marquise.Types 
import Pipes
import Vaultaire.Types

-- | Create a name in the spool. Only alphanumeric characters are allowed, max length
-- is 32 characters.
makeSpoolName :: String -> Either String SpoolName
makeSpoolName s
    | any (not . isAlphaNum) s = Left "non-alphanumeric spool name"
    | otherwise = Right $ SpoolName s

-- | Deterministically convert a ByteString to an Address, this uses siphash.
hashIdentifier :: ByteString -> Address
hashIdentifier = Address . (`clearBit` 0) . unSipHash . hash iv
  where
    iv = SipKey 0 0
    unSipHash (SipHash h) = h :: Word64

-- | Generate an un-used Address. You will need to store this for later re-use.
requestUnique :: MarquiseContentsMonad m connection
               => Origin
               -> connection
               -> m (Either SomeException Address)
requestUnique origin connection =  do
    sendContentsRequest GenerateNewAddress origin connection
    response <- recvContentsResponse connection
    return $ case response of
        Right (RandomAddress addr) ->
            Right addr
        Right _ -> error "requestUnique: Invalid response"
        Left e -> Left e

-- | Set the key,value tags as metadata on the given Address.
updateSourceDict :: MarquiseContentsMonad m connection
                 => Address
                 -> SourceDict
                 -> Origin
                 -> connection
                 -> m (Either SomeException ())
updateSourceDict addr source_dict origin connection =  do
    sendContentsRequest (UpdateSourceTag addr source_dict) origin connection
    response <- recvContentsResponse connection
    return $ case response of
        Right UpdateSuccess -> Right ()
        Right _ -> error "requestSourceDictUpdate: Invalid response"
        Left e -> Left e

-- | Remove the supplied key,value tags from metadata on the Address, if present.
removeSourceDict :: MarquiseContentsMonad m connection
                 => Address
                 -> SourceDict
                 -> Origin
                 -> connection
                 -> m (Either SomeException ())
removeSourceDict addr source_dict origin connection = do
    sendContentsRequest (RemoveSourceTag addr source_dict) origin connection
    response <- recvContentsResponse connection
    return $ case response of
        Right RemoveSuccess -> Right ()
        Right _ -> error "requestSourceDictRemoval: Invalid response"
        Left e -> Left e

enumerateOrigin :: MarquiseContentsMonad m connection
                => Origin
                -> connection
                -> Producer (Address, SourceDict) m ()
enumerateOrigin origin connection = do
    lift $ sendContentsRequest ContentsListRequest origin connection
    loop
  where
    loop = do
        resp <- lift $ recvContentsResponse connection
        case resp of
            Left e -> error $ show e
            Right (ContentsListEntry addr dict) ->
                yield (addr, dict) >> loop
            Right EndOfContentsList ->
                return ()
            Right _ ->
                error "enumerateOrigin loop: Invalid response"


readSimple :: Monad m => Address -> Word64 -> Word64 -> Producer SimpleBurst m ()
readSimple = undefined

readExtended :: Monad m => Address -> Word64 -> Word64 -> Producer ExtendedBurst m ()
readExtended = undefined

decodeSimpleBurst :: Monad m => Pipe SimpleBurst SimplePoint m ()
decodeSimpleBurst = undefined

decodeExtendedBurst :: Monad m => Pipe ExtendedBurst ExtendedPoint m ()
decodeExtendedBurst = undefined


-- | Send a "simple" data point. Interpretation of this point, e.g.
-- float/signed is up to you, but it must be sent in the form of a Word64.
sendSimple
    :: MarquiseSpoolFileMonad m
    => SpoolName
    -> Address
    -> TimeStamp
    -> Word64
    -> m ()
sendSimple ns (Address ad) (TimeStamp ts) w = append ns bytes
  where
    bytes = LB.fromStrict $ runPacking 24 $ do
        putWord64LE ad
        putWord64LE ts
        putWord64LE w

-- | Send an "extended" data point. Again, representation is up to you.
sendExtended
    :: MarquiseSpoolFileMonad m
    => SpoolName
    -> Address
    -> TimeStamp
    -> ByteString
    -> m ()
sendExtended ns (Address ad) (TimeStamp ts) bs = append ns bytes
  where
    len = BS.length bs
    bytes = LB.fromStrict $ runPacking (24 + len) $ do
        putWord64LE ad
        putWord64LE ts
        putWord64LE $ fromIntegral len
        putBytes bs

-- | Ensure that all sent points have hit the local disk.
flush
    :: MarquiseSpoolFileMonad m
    => SpoolName
    -> m ()
flush = close

