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
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}

module Vaultaire.Conversion.Writer (
    DiskHeader(..),
    Compression(..),
    Quantity(..),
    createDiskContent,
    createDiskPoint,
    encodePoint
) where

--
-- Code begins
--

import Data.Bits
import qualified Data.ByteString.Char8 as S
import qualified Data.Map.Strict as Map
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import Data.Text (Text)
import Data.Word

import qualified Vaultaire.Internal.CoreTypes as Core
import qualified Vaultaire.Serialize.DiskFormat as Protobuf


{-
The code here is a direct adaptation of what was originally prototyped for
DataFrame; as that represents the entire data schema and was written first,
see there for more cohesive comments.
-}

data Compression = Normal | Compressed
data Quantity = Single | Multiple

data DiskHeader = DiskHeader {
    version     :: Word8,
    compression :: Compression,
    quantity    :: Quantity,
    size        :: Word64
}

deriving instance Show Quantity
deriving instance Show Compression
deriving instance Show DiskHeader

--
-- e v v v c m s s
-- 0 0 0 1 0 0 0 1  version 1, uncompressed, single item, 1 byte size
--

instance Serialize DiskHeader where

--  get :: Get DiskHeader
    get = do
        b <- getWord8

        let w = b .&. 0x03       -- number of size bytes, 2^w
        s <- case w of
                    0x00    -> do
                                x <- getWord8
                                return $ fromIntegral x
                    0x01    -> do
                                x <- getWord16le
                                return $ fromIntegral x
                    0x02    -> do
                                x <- getWord32le
                                return $ fromIntegral x
                    0x03    -> do
                                x <- getWord64le
                                return x
                    _       -> error "FIXME"

        let c = b .&. 0x08                  -- compression bit

        let q = b .&. 0x04                  -- quantity bit

        let v = (b .&. 0x70) `shiftR` 4     -- version bits

        let e = b .&. 0x80                  -- extension bit

        return $ DiskHeader {
                    version = v,
                    compression = case c of
                                    0x0 -> Normal
                                    0x8 -> Compressed
                                    _   -> error "Illegal compression",
                    quantity    = case q of
                                    0x0 -> Single
                                    0x4 -> Multiple
                                    _   -> error "Illegal quantity",
                    size = s
                }


--  put :: Putter DiskHeader
    put x = do
        let e = if version x > 7
            then error "Unimplemented for version > 7"
            else 0x00

        let v = (version x) `shiftL` 4

        let c = case compression x of
                    Normal      -> 0x00
                    Compressed  -> 0x08

        let q = case quantity x of
                    Single      -> 0x00
                    Multiple    -> 0x04

        let w   | size x < 256        = 0x00
                | size x < 65536      = 0x01
                | size x < 4294967296 = 0x02
                | otherwise           = 0x03

        let b = e .|. v .|. c .|. q .|. w

        putWord8 b

        case w of
            0x00    -> putWord8 (fromIntegral $ size x)
            0x01    -> putWord16le (fromIntegral $ size x)
            0x02    -> putWord32le (fromIntegral $ size x)
            0x03    -> putWord64le (fromIntegral $ size x)
            _       -> error "Illegal width"


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



encodePoint :: Protobuf.VaultPoint -> S.ByteString
encodePoint x = runPut $ encodeMessage x

