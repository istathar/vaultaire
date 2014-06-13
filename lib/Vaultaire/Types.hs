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
    ReadRequest(..),
    ReadStream(..),

    -- * Writes
    WriteResult(..),

    -- * Hacks
    PassThrough(..),

    -- * Conversion to and from wire format
    WireFormat(fromWire, toWire),

    -- * Convenience/clarity
    Epoch,
    NumBuckets,
    DayMap,
    Time,
) where

import Vaultaire.Classes.WireFormat
import Vaultaire.Types.Address
import Vaultaire.Types.Common
import Vaultaire.Types.ContentsListBypass
import Vaultaire.Types.ContentsOperation
import Vaultaire.Types.ContentsResponse
import Vaultaire.Types.PassThrough
import Vaultaire.Types.ReadRequest
import Vaultaire.Types.ReadStream
import Vaultaire.Types.SourceDict
import Vaultaire.Types.WriteResult

