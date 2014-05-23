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
    makeNameSpace,
    -- | * Request or assign Addresses
    requestUnique,
    hashIdentifier,
    sendSimple,
    sendExtended,
    flush,
    -- * Types
    NameSpace,
    Address,
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import Data.Char (isAlphaNum)
import Data.Packer (putBytes, putWord64LE, runPacking)
import Data.Word (Word64)
import Marquise.IO (ContentsClientMonad (..), MarquiseClientMonad (..))
import Marquise.Types (NameSpace (..), TimeStamp (..))
import Vaultaire.CoreTypes (Address (..))

-- | Create a namespace, only alphanumeric characters are allowed, max length
-- is 32 characters.
makeNameSpace :: String -> Either String NameSpace
makeNameSpace s
    | any (not . isAlphaNum) s = Left "non-alphanumeric namespace"
    | otherwise = Right $ NameSpace s


-- | For the case where you can track Addresses yourself, we provide a facility
-- to generate unique ones for you, guaranteed free of collision.
requestUnique :: ContentsClientMonad m => m Address
requestUnique = requestUniqueAddress

-- | If you have deterministic or fixed known identifiers for your sources, you can
-- use this function to translate it as an Address.
hashIdentifier :: ByteString -> Address
hashIdentifier = undefined


-- | Send a "simple" data point. Interpretation of this point, e.g.
-- float/signed is up to you, but it must be sent in the form of a Word64.
sendSimple
    :: MarquiseClientMonad m
    => NameSpace
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
    => NameSpace
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
    => NameSpace
    -> m ()
flush = close
