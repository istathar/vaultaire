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
{-# OPTIONS -fno-warn-unused-imports #-}

module Receiver (
    convertToProtobuf,
    convertToPoint,
    decodeBurst,
    encodePoints,
) where

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--


--
-- Code begins
--

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Hex
import Data.Int (Int64)
import Data.List (intercalate)
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Data.Typeable (Typeable)
import Data.Word (Word32, Word64)

import qualified CoreTypes as Core
import qualified WireFormat as Protobuf


--
-- Conversion from our internal types to a the Data.Protobuf representation,
-- suitable for subsequent encoding.
--
convertToProtobuf :: Core.Point -> Protobuf.DataFrame
convertToProtobuf p =
  let
    tags =
           Map.elems $ Map.mapWithKey createSourceTag (Core.source p)
  in
    case Core.payload p of
        Core.Empty       ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp p,
                Protobuf.payload = putField Protobuf.EMPTY,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Numeric n   ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp p,
                Protobuf.payload = putField Protobuf.NUMBER,
                Protobuf.valueNumeric = putField (Just n),
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Measurement r ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp p,
                Protobuf.payload = putField Protobuf.REAL,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = putField (Just r),
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Textual t   ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp p,
                Protobuf.payload = putField Protobuf.TEXT,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = putField (Just t),
                Protobuf.valueBlob = mempty
            }
        Core.Blob b'     ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp p,
                Protobuf.payload = putField Protobuf.BINARY,
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


--
-- Given an protobuf, convert it to our internal Point representation. This
-- completes use of the Data.Protobuf library; from here we're in normal
-- Haskell types.
--
convertToPoint :: Protobuf.DataFrame -> Core.Point
convertToPoint x =
  let
    v = case (getField $ Protobuf.payload x) of
        Protobuf.EMPTY   -> Core.Empty
        Protobuf.NUMBER  -> Core.Numeric (fromMaybe 0 $ getField $ Protobuf.valueNumeric x)
        Protobuf.REAL    -> Core.Measurement (fromMaybe 0.0 $ getField $ Protobuf.valueMeasurement x)
        Protobuf.TEXT    -> Core.Textual (fromMaybe T.empty $ getField $ Protobuf.valueTextual x)
        Protobuf.BINARY  -> Core.Blob (fromMaybe S.empty $ getField $ Protobuf.valueBlob x)
    ss = getField $ Protobuf.source x       :: [Protobuf.SourceTag]
    as = map createMapEntry ss              :: [(Text,Text)]
  in
    Core.Point {
        Core.source = Map.fromList as,
        Core.timestamp = getField (Protobuf.timestamp x),
        Core.payload = v
    }


createMapEntry :: Protobuf.SourceTag -> (Text,Text)
createMapEntry tag =
  let
    k = getField $ Protobuf.field tag
    v = getField $ Protobuf.value tag
  in
    (k,v)

{-
    Encoding and decoding. This is phrased in terms of lists at the moment,
    which will likely be horribly inefficient at any kind of scale. If so we
    can switch to Vectors (if allocation is the problem) and/or io-streams (if
    we need streaming).
-}

encodeFrame :: Protobuf.DataFrame -> S.ByteString
encodeFrame x = runPut $ encodeMessage x


decodeFrame :: S.ByteString -> Either String Protobuf.DataFrame
decodeFrame x' = runGet decodeMessage x'



decodeBurst :: S.ByteString -> Either String [Core.Point]
decodeBurst y' =
  let
    ey = runGet decodeMessage y' :: Either String Protobuf.DataBurst
  in
    case ey of
        Left err    -> Left err
        Right y     -> Right $ convertToPoints y



convertToPoints :: Protobuf.DataBurst -> [Core.Point]
convertToPoints y =
  let
    xs = getField $ Protobuf.frames y
    ps = List.map convertToPoint xs
  in
    ps


encodePoints :: [Core.Point] -> S.ByteString
encodePoints ps =
  let
    xs = List.map convertToProtobuf ps
    y  = Protobuf.DataBurst {
            Protobuf.frames = putField xs
         }
  in
    runPut $ encodeMessage y

