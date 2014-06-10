--
-- |
-- Maintainer: The Vaultaire Team
-- Stability: Experimental
--
-- /Overview/
--
-- When communicating with a Vaultaire installation, you need to serialize
-- requests and deserialize responses. The bytes used over the wire are all
-- formed by making the various types involved instances of class 'WireFormat'.
--
-- As it happens, these types are also the same ones used in the internals of
-- Vaultaire as it persists measurements and metadata to disk.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Vaultaire.Types
(
    -- * Identification of measurements
    Address(..),
    encodeAddressToString,
    decodeStringAsAddress,
    calculateBucketNumber,
    isAddressExtended,

    -- * Namespacing and authentication
    Origin(..),

    -- * Metadata about sources
    SourceDict,
    unionSource,
    diffSource,
    makeSourceDict,

    -- * Operations with the contents store
    ContentsOperation(..),
    ContentsResponse(..),
    ContentsListBypass(..),

    -- * Streaming reads
    ReadStream(..),

    -- * Writes
    WriteResult(..),

    -- * Conversion to and from wire format
    WireFormat(fromWire, toWire)
) where

import Vaultaire.Classes.WireFormat
import Vaultaire.Types.Address
import Vaultaire.Types.ContentsListBypass
import Vaultaire.Types.ContentsOperation
import Vaultaire.Types.ContentsResponse
import Vaultaire.Types.ReadStream
import Vaultaire.Types.SourceDict
import Vaultaire.Types.WriteResult

import Data.ByteString (ByteString)
import Data.Hashable (Hashable)
import Data.String (IsString)

newtype Origin = Origin { unOrigin :: ByteString }
    deriving (Eq, Ord, IsString, Hashable, Show)
