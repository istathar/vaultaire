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

{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Vaultaire.Conversion.Writer (
    createDiskPrefix,
    createDiskContent,
    createDiskPoint,
    encodePoint
) where

--
-- Code begins
--

import qualified Data.ByteString.Char8 as S
import Data.Int (Int64)
import qualified Data.Map.Strict as Map
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import Data.Text (Text)

import qualified Vaultaire.Internal.CoreTypes as Core
import qualified Vaultaire.Serialize.DiskFormat as Protobuf
import qualified Vaultaire.Serialize.DiskFormat as Disk

createDiskPrefix :: Int64 -> Disk.VaultPrefix
createDiskPrefix n =
    Disk.VaultPrefix {
        Disk.extended = False,
        Disk.version = 0,
        Disk.compression = Disk.Normal,
        Disk.quantity = Disk.Single,
        Disk.size = fromIntegral n
    }


{-
The code here is a direct adaptation of what was originally prototyped for
DataFrame; as that represents the entire data schema and was written first,
see there for more cohesive comments.
-}

--
-- Conversion from our internal types to a the Data.Protobuf representation,
-- suitable for subsequent encoding.
--


createDiskContent :: Core.Point -> Protobuf.VaultContent
createDiskContent p =
  let
    tags =
           Map.elems $ Map.mapWithKey createSourceTag (Core.source p)
  in
    case Core.payload p of
        Core.Empty       ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "FIXME",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.EMPTY
            }
        Core.Numeric _   ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "FIXME",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.NUMBER
            }
        Core.Measurement _ ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "FIXME",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.REAL
            }
        Core.Textual _   ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "FIXME",
                Protobuf.source = putField tags,
                Protobuf.payload = putField Protobuf.TEXT
            }
        Core.Blob _      ->
            Protobuf.VaultContent {
                Protobuf.origin = putField $ S.pack "FIXME",
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



encodePoint :: Protobuf.VaultPoint -> S.ByteString
encodePoint x = runPut $ encodeMessage x

