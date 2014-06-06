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
module Marquise.Client
(
    -- | * Functions
    -- Note: You may read MarquiseClientMonad m as IO.
    makeSpoolName,
    withBroker,

    -- | * Request or assign Addresses
    requestUnique,
    hashIdentifier,
    makeSourceDict,
    updateSourceDict,
    removeSourceDict,

    -- | * Sending data to Vaultaire
    sendSimple,
    sendExtended,
    flush,
    -- * Types
    SpoolName,
    Address,
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
import Marquise.IO (ContentsClientMonad (..), ContentsConfig (..),
                    MarquiseClientMonad (..))
import Marquise.Types (SpoolName (..), TimeStamp (..))
import Pipes (Producer)
import Vaultaire.Types (Address (..), Origin, SourceDict, makeSourceDict)

-- | Create a name in the spool. Only alphanumeric characters are allowed, max length
-- is 32 characters.
makeSpoolName :: String -> Either String SpoolName
makeSpoolName s
    | any (not . isAlphaNum) s = Left "non-alphanumeric spool name"
    | otherwise = Right $ SpoolName s

withBroker :: Monad m => String -> Origin -> ReaderT ContentsConfig m a -> m a
withBroker broker origin action = runReaderT action (ContentsConfig broker origin)

-- | Generate an un-used Address. You will need to store this for later re-use.
requestUnique :: ContentsClientMonad m => m (Either SomeException Address)
requestUnique = requestUniqueAddress

-- | Deterministically convert a ByteString to an Address, this uses siphash.
hashIdentifier :: ByteString -> Address
hashIdentifier = Address . (`clearBit` 0) . unSipHash . hash iv
  where
    iv = SipKey 0 0
    unSipHash (SipHash h) = h :: Word64

-- | Set the key,value tags as metadata on the given Address.
updateSourceDict :: ContentsClientMonad m
                 => Address
                 -> SourceDict
                 -> m (Either SomeException ())
updateSourceDict = requestSourceDictUpdate

-- | Remove the supplied key,value tags from metadata on the Address, if present.
removeSourceDict :: ContentsClientMonad m
                 => Address
                 -> SourceDict
                 -> m (Either SomeException ())
removeSourceDict = requestSourceDictRemoval

enumerateOrigin :: ContentsClientMonad m
                => Producer (Address, SourceDict) m ()
enumerateOrigin = requestList

-- | Send a "simple" data point. Interpretation of this point, e.g.
-- float/signed is up to you, but it must be sent in the form of a Word64.
sendSimple
    :: MarquiseClientMonad m
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
    :: MarquiseClientMonad m
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
    :: MarquiseClientMonad m
    => SpoolName
    -> m ()
flush = close
