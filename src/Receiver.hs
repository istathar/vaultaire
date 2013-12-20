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
    convertToProtobuf, convertToPoint
) where

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Debug.Trace

--
-- Code begins
--

import Data.Hex
import Data.Int (Int64)
import Data.List (intercalate)
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
convertToProtobuf x =
  let
    tags =
           Map.elems $ Map.mapWithKey createSourceTag (Core.source x)
  in
    case Core.payload x of
        Core.Empty       ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp x,
                Protobuf.payload = putField Protobuf.EMPTY,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Numeric n   ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp x,
                Protobuf.payload = putField Protobuf.NUMBER,
                Protobuf.valueNumeric = putField (Just n),
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Measurement r ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp x,
                Protobuf.payload = putField Protobuf.REAL,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = putField (Just r),
                Protobuf.valueTextual = mempty,
                Protobuf.valueBlob = mempty
            }
        Core.Textual t   ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp x,
                Protobuf.payload = putField Protobuf.TEXT,
                Protobuf.valueNumeric = mempty,
                Protobuf.valueMeasurement = mempty,
                Protobuf.valueTextual = putField (Just t),
                Protobuf.valueBlob = mempty
            }
        Core.Blob b'     ->
            Protobuf.DataFrame {
                Protobuf.source = putField tags,
                Protobuf.timestamp = putField $ Core.timestamp x,
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
-- | Given an protobuf, convert it to our internal Point representation. This
-- completes use of the Data.Protobuf library; from here we're in normal
-- Haskell types.
--
convertToPoint :: Protobuf.DataFrame -> Core.Point
convertToPoint y =
  let
    v = case (getField $ Protobuf.payload y) of
        Protobuf.EMPTY   -> Core.Empty
        Protobuf.NUMBER  -> Core.Numeric (fromMaybe 0 $ getField $ Protobuf.valueNumeric y)
        Protobuf.REAL    -> Core.Measurement (fromMaybe 0.0 $ getField $ Protobuf.valueMeasurement y)
        Protobuf.TEXT    -> Core.Textual (fromMaybe T.empty $ getField $ Protobuf.valueTextual y)
        Protobuf.BINARY  -> Core.Blob (fromMaybe S.empty $ getField $ Protobuf.valueBlob y)
    ss = getField $ Protobuf.source y        :: [Protobuf.SourceTag]
    as = map createMapEntry ss      :: [(Text,Text)]
  in
    Core.Point {
        Core.source = Map.fromList as,
        Core.timestamp = getField (Protobuf.timestamp y),
        Core.payload = v
    }


createMapEntry :: Protobuf.SourceTag -> (Text,Text)
createMapEntry tag =
  let
    k = getField $ Protobuf.field tag
    v = getField $ Protobuf.value tag
  in
    (k,v)

