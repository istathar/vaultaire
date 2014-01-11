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
{-# OPTIONS -fno-warn-orphans #-}

module TestSuite where

import Test.Hspec
import Test.Hspec.QuickCheck
import Test.HUnit
import Test.QuickCheck (elements, property)
import Test.QuickCheck.Arbitrary (Arbitrary, arbitrary)

import Control.Monad
import Data.Word

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
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

import Vaultaire.Conversion.Reader
import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Conversion.Writer
import qualified Vaultaire.Internal.CoreTypes as Core
import Vaultaire.Persistence.Locators
import qualified Vaultaire.Persistence.ObjectFormat as Bucket
import Vaultaire.Serialize.DiskFormat (Compression (..), Quantity (..))
import qualified Vaultaire.Serialize.DiskFormat as Disk
import qualified Vaultaire.Serialize.WireFormat as Protobuf

suite :: Spec
suite = do
    describe "a DataFrame protobuf" $ do
        testSerializeDataFrame
        testConvertPoint
        testReadFrame

    describe "on-disk VaultPrefix" $ do
        testSerializeVaultHeader
        testRoundTripVaultHeader

    describe "a VaultPoint protobuf" $ do
        testSerializeVaultPoint

    describe "objects in vault" $ do
        testRoundTripLocator16
        testKnownLocator16a
        testFormBucketLabel



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
                    Protobuf.origin = putField (Just "perf_data"),
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
  let
    tags = Map.fromList
           [("origin", "bletchley/testframe"),
            ("hostname", "hut4"),
            ("service_name", "bombe"),
            ("metric", "runtime")]

    msg = Core.Point {
        Core.origin = S.empty,
        Core.source = tags,
        Core.timestamp = 1388482381430911607,
        Core.payload = Core.Measurement 8.461049696649084
    }
  in
    it "serializes a Core.Point to correct protobuf" $ do
        let x1 = createDataFrame msg
        let x1' = runPut $ encodeMessage x1

        x0' <- S.readFile "tests/data/output_DataFrame.pb"
        assertEqual "Incorrect message content" x0' x1'


testReadFrame =
  let
    tags = Map.fromList
           [("origin", "bletchley/testframe"),
            ("hostname", "hut4"),
            ("service_name", "bombe"),
            ("metric", "runtime")]

    msg = Core.Point {
        Core.origin = S.empty,
        Core.source = tags,
        Core.timestamp = 1388482381430911607,
        Core.payload = Core.Measurement 8.461049696649084
    }
  in
    it "deserializes an externally supplied DataFrame" $ do
        x' <- S.readFile "tests/data/bletchley_DataFrame.pb"

        let ex = runGet decodeMessage x' :: Either String Protobuf.DataFrame
        case ex of
            Left err -> assertFailure err
            Right x  -> let
                            p = convertToPoint x
                        in
                            assertEqual "Decoded protobuf not as expected" msg p


testSerializeVaultHeader =
  let
    h1 = Disk.VaultPrefix {
                Disk.extended = False,
                Disk.version = 7,
                Disk.compression = Disk.Compressed,
                Disk.quantity = Disk.Multiple,
                Disk.size = 42
            }
  in do
    it "serializes to the correct bytes" $ do
        let h' = encode h1

        assertEqual "Incorrect number of bytes" 2 (B.length h')
        assertEqual "Incorrect serialization" [0x7c,0x2a] (B.unpack h')

    it "deserializes to the correct object" $ do
        let h' = B.pack [0x7c,0x2a]

        let eh2 = decode h'

        case eh2 of
            Left err    -> assertFailure err
            Right h2    -> assertEqual "Incorrect deserialization" h1 h2

instance Arbitrary Disk.Word3 where
    arbitrary = elements [0..7]

instance Arbitrary Disk.Compression where
    arbitrary = elements [Disk.Normal, Disk.Compressed]

instance Arbitrary Disk.Quantity where
    arbitrary = elements [Disk.Single, Disk.Multiple]

instance Arbitrary Disk.VaultPrefix where
    arbitrary = liftM5 Disk.VaultPrefix arbitrary arbitrary arbitrary arbitrary arbitrary

testRoundTripVaultHeader =
    prop "round-trips correctly at boundaries" prop_RoundTrip

prop_RoundTrip :: Disk.VaultPrefix -> Bool
prop_RoundTrip prefix =
  let
    decoded = either error id $ decode (encode prefix)
  in
    prefix == decoded

testSerializeVaultPoint =
  let
    tags = Map.fromList
           [("hostname", "secure.example.org"),
            ("metric", "eth0-tx-bytes"),
            ("datacenter", "lhr1"),
            ("epoch", "1")]

    p1 = Core.Point {
        Core.origin = "perf_data",
        Core.source = tags,
        Core.timestamp = 1386931666289201468,
        Core.payload = Core.Numeric 201468
--      payload = Core.Textual "203.0.113.101 - - [12/Dec/2013:04:11:16 +1100] \"GET /the-politics-of-praise-william-w-young-iii/prod9780754656463.html HTTP/1.1\" 200 15695 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""
--      payload = Core.Measurement 45.9
    }
  in
    it "serializes Core.Point to Disk.VaultPoint " $ do
        let pb1 = createDiskPoint p1
        let p1' = runPut $ encodeMessage pb1

        assertEqual "Incorrect length" 15 (S.length p1')

        let p2' = p1'
        let epb2 = runGet decodeMessage p2'
        case epb2 of
            Left err    -> assertFailure err
            Right pb2   -> do
                assertEqual "Incorrect de-serialization" pb1 pb2

                let cb2 = undefined

                let p2 = convertVaultToPoint cb2 pb2

                pendingWith "Define VaultContents conversion code"
                assertEqual "Point object converted not equal to original object" p1 p2


testFormBucketLabel =
  let
{-
    b1 = Core.Bucket {
        Core.origin = "perf_data",
        Core.source2 = tags,
        Core.timemark = 1388400000
    }
-}
    t1 = Map.fromList
           [("hostname", "web01.example.com"),
            ("metric", "math-constants"),
            ("datacenter", "lhr1")]

    p1 = Core.Point {
        Core.origin = "arithmetic/127.0.0.1",
        Core.source = t1,
        Core.timestamp = 1387929601271828182,       -- 25 Dec + e
        Core.payload = Core.Measurement 2.718281    -- e
    }

    t2 = Map.fromList
           [("metric", "math-constants"),
            ("datacenter", "lhr1"),
            ("hostname", "web01.example.com")]

    p2 = Core.Point {
        Core.origin = "arithmetic/127.0.0.1",
        Core.source = t2,
        Core.timestamp = 1387929601314159265,       -- 25 Dec + pi
        Core.payload = Core.Measurement 3.141592    -- pi
    }

  in do
    it "correctly forms an object label" $ do
        let l1 = Bucket.formObjectLabel p1
        assertEqual "Incorrect label"
            (S.pack "01_XK9Y10_5uzXcmefmp7RtQKcPqVLiAQgAUB_1387900000") l1

    it "two labels in same mark match" $ do
        let l1 = Bucket.formObjectLabel p1
        let l2 = Bucket.formObjectLabel p2
        assertEqual "Map should be sorted, time mark div 10^6" l1 l2


testRoundTripLocator16 =
    prop "safe conversion to/from Locator16" prop_Locator16

prop_Locator16 :: Int -> Bool
prop_Locator16 i =
  let
    n = abs i
    decoded = fromLocator16 (toLocator16 n)
  in
    n == decoded


--
-- Have to do these manually, since Locator16a is not round-trip safe.
--
testKnownLocator16a =
    it "constrains Locator16a to unique digits" $ do
        assertEqual "Incorrect result" "12C4FH" (toLocator16a 0x111111)
        assertEqual "Incorrect result" "789KLM" (toLocator16a 0x777777)
        assertEqual "Incorrect result" "MRXY01" (toLocator16a 0xCCCCCC)


