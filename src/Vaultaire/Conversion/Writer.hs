--
-- Data vault for metrics
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}

module Vaultaire.Conversion.Writer (
    createDiskHeader,
    createDiskContent,
    createDiskPoint
) where

--
-- Code begins
--

import Data.Bits
import qualified Data.ByteString.Char8 as S
import qualified Data.Map.Strict as Map
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (field)
import Data.Text (Text)
import Data.Word (Word32)

import qualified Vaultaire.Internal.CoreTypes as Core
import qualified Vaultaire.Serialize.DiskFormat as Protobuf


{-
The code here is a direct adaptation of what was originally prototyped for
DataFrame; as that represents the entire data schema and was written first,
see there for more cohesive comments.
-}

type Compression = Bool

--
-- Conversion from our internal types to a the Data.Protobuf representation,
-- suitable for subsequent encoding.
--
createDiskHeader :: Compression -> Int -> Protobuf.VaultHeader
createDiskHeader c n =
  let
    flags = if c
                then 1
                else 0
    size  = if n > 16777215
                then error "Size too large"
                else fromIntegral n :: Word32
    bits  = (flags `shiftL` 24) .|. size
  in
    Protobuf.VaultHeader {
        Protobuf.header = putField $ Fixed bits
    }


createDiskContent :: Core.Point -> Protobuf.VaultContent
createDiskContent p =
  let
    tags =
           Map.elems $ Map.mapWithKey createSourceTag (Core.source p)
  in
    case Core.payload p of
        Core.Empty       ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "0001",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.EMPTY
            }
        Core.Numeric _   ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "0001",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.NUMBER
            }
        Core.Measurement _ ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "0001",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.REAL
            }
        Core.Textual _   ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "0001",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.TEXT
            }
        Core.Blob _      ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "0001",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.BINARY
            }


createDiskPoint :: Core.Point -> Protobuf.VaultPoint
createDiskPoint p =
    case Core.payload p of
        Core.Empty       ->
            Protobuf.VaultPoint {
                Protobuf.timestamp = putField $ Fixed $ Core.timestamp p,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Numeric n   ->
            Protobuf.VaultPoint {
                Protobuf.timestamp = putField $ Fixed $ Core.timestamp p,
                Protobuf.valueNumeric = putField (Just n),
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Measurement r ->
            Protobuf.VaultPoint {
                Protobuf.timestamp = putField $ Fixed $ Core.timestamp p,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = putField (Just r),
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Textual t   ->
            Protobuf.VaultPoint {
                Protobuf.timestamp = putField $ Fixed $ Core.timestamp p,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = putField (Just t),
                Protobuf.valueBlob = mempty
            }
        Core.Blob b'     ->
            Protobuf.VaultPoint {
                Protobuf.timestamp = putField $ Fixed $ Core.timestamp p,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = putField (Just b')
            }


createSourceTag :: Text -> Text -> Protobuf.SourceTag
createSourceTag k v =
    Protobuf.SourceTag {
        Protobuf.field = putField k,
        Protobuf.value = putField v
    }


