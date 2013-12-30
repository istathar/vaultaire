--
-- Data vault for metrics
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module TestSuite where

import Test.Hspec
import Test.HUnit

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import qualified Data.Map.Strict as Map
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (decode, encode)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Debug.Trace

--
-- What we're actually testing.
--

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Writer
import qualified Vaultaire.Internal.CoreTypes as Core
import qualified Vaultaire.Serialize.DiskFormat as Disk
import qualified Vaultaire.Serialize.WireFormat as Protobuf

suite :: Spec
suite = do
    describe "a DataFrame protobuf" $ do
        testSerializeDataFrame
        testConvertPoint

    describe "on-disk VaultPrefix" $ do
        testSerializeVaultHeader


testSerializeDataFrame =
    it "serializes to the correct bytes" $ do
        let gs =
               [Protobuf.SourceTag {
                    Protobuf.field = putField "hostname",
                    Protobuf.value = putField "secure.example.org"
                },
                Protobuf.SourceTag {
                    Protobuf.field = putField "metric",
                    Protobuf.value = putField "eth0-tx-bytes"
                },
                Protobuf.SourceTag {
                    Protobuf.field = putField "datacenter",
                    Protobuf.value = putField "lhr1"
                }]

        let x =
                Protobuf.DataFrame {
                    Protobuf.source = putField gs,
                    Protobuf.timestamp = putField 1387524524342329774,
                    Protobuf.payload = putField Protobuf.NUMBER,
                    Protobuf.valueNumeric = putField (Just 45007),
                    Protobuf.valueMeasurement = mempty,
                    Protobuf.valueTextual = mempty,
                    Protobuf.valueBlob = mempty
                }

        let x' = runPut $ encodeMessage x

        assertEqual "incorrect bytes!" x' x'


testConvertPoint =
    it "serializes a Core.Point to a Protobuf.DataFrame" $ do
        let tags = Map.fromList
               [("hostname", "secure.example.org"),
                ("metric", "eth0-tx-bytes"),
                ("datacenter", "lhr1"),
                ("epoch", "1")]

        let msg = Core.Point {
            Core.source = tags,
            Core.timestamp = 1386931666289201468,
            Core.payload = Core.Numeric 201468
        }

        let x = undefined
        pending

{-
    let msgs = [msg, msg, msg]

    let burst = DataBurst {
        frames = putField msgs
    }
-}


testSerializeVaultHeader = do
    it "serializes to the correct bytes" $ do
        let h = Disk.VaultPrefix {
                    Disk.version = 7,
                    Disk.compression = Disk.Compressed,
                    Disk.quantity = Disk.Multiple,
                    Disk.size = 42
                }
        let h' = encode h

        assertEqual "Incorrect number of bytes" 2 (S.length h')
        assertEqual "Incorrect serialization" [0x7c,0x2a] (S.unpack h')

    it "deserializes to the correct object" $ do
        let h1 = Disk.VaultPrefix {
                    Disk.version = 7,
                    Disk.compression = Disk.Compressed,
                    Disk.quantity = Disk.Multiple,
                    Disk.size = 42
                }

        let h' = S.pack [0x7c,0x2a]

        let eh2 = decode h'

        case eh2 of
            Left err    -> assertFailure err
            Right h2    -> assertEqual "Incorrect deserialization" h1 h2

