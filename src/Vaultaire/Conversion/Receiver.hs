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

module Vaultaire.Conversion.Receiver (
    convertToPoint,
    decodeBurst
) where

--
-- Code begins
--

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import qualified Data.Text as T

import qualified Vaultaire.Internal.CoreTypes as Core
import qualified Vaultaire.Serialize.WireFormat as Protobuf


--
-- Given a DataFrame protobuf, convert it to our internal Point representation.
-- This completes use of the Data.Protobuf library on the ingest side; from
-- here we're in normal Haskell types.
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
    ss = getField $ Protobuf.source x          :: [Protobuf.SourceTag]
    as = map convertToMapEntry ss              :: [(ByteString,ByteString)]
    (Fixed m) = getField (Protobuf.timestamp x)
    o' = fromMaybe "" $ getField (Protobuf.origin x)
  in
    Core.Point {
        Core.origin = Core.Origin o',
        Core.source = Core.SourceDict $ Map.fromList as,
        Core.timestamp = m,
        Core.payload = v
    }


convertToMapEntry :: Protobuf.SourceTag -> (ByteString,ByteString)
convertToMapEntry tag =
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


convertToPoints :: Protobuf.DataBurst -> [Core.Point]
convertToPoints y =
  let
    xs = getField $ Protobuf.frames y
    ps = List.map convertToPoint xs
  in
    ps


decodeBurst :: ByteString -> Either String [Core.Point]
decodeBurst y' =
  let
    ey = runGet decodeMessage y'
  in
    case ey of
        Left err    -> Left err
        Right y     -> Right $ convertToPoints y


