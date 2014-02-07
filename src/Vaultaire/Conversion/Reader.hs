--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}

module Vaultaire.Conversion.Reader (
    convertVaultToPoint,
    decodeSingle,
    convertVaultToSource,
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import qualified Data.Text as T

import qualified Vaultaire.Internal.CoreTypes as Core
import qualified Vaultaire.Serialize.DiskFormat as Protobuf


--
-- Given a DataFrame protobuf, convert it to our internal Point representation.
-- This completes use of the Data.Protobuf library on the ingest side; from
-- here we're in normal Haskell types.
--
{-
    ss = getField $ Protobuf.source cb         :: [Protobuf.SourceTag]
    as = map convertToMapEntry ss              :: [(Text,Text)]
  in
    ...
    Core.source = Map.fromList as,

convertToMapEntry :: Protobuf.SourceTag -> (Text,Text)
convertToMapEntry tag =
  let
    k = getField $ Protobuf.field tag
    v = getField $ Protobuf.value tag
  in
    (k,v)
-}

convertVaultToPoint :: Core.Origin -> Core.SourceDict -> Protobuf.VaultPoint -> Core.Point
convertVaultToPoint o s pb =
  let
    v = case (getField $ Protobuf.payload pb) of
        Protobuf.EMPTY   -> Core.Empty
        Protobuf.NUMBER  -> Core.Numeric (fromMaybe 0 $ getField $ Protobuf.valueNumeric pb)
        Protobuf.REAL    -> Core.Measurement (fromMaybe 0.0 $ getField $ Protobuf.valueMeasurement pb)
        Protobuf.TEXT    -> Core.Textual (fromMaybe T.empty $ getField $ Protobuf.valueTextual pb)
        Protobuf.BINARY  -> Core.Blob (fromMaybe S.empty $ getField $ Protobuf.valueBlob pb)
    (Fixed m) = getField (Protobuf.timestamp pb)
  in
    Core.Point {
        Core.origin = o,
        Core.source = s,
        Core.timestamp = m,
        Core.payload = v
    }



{-
    Encoding and decoding.
-}




decodeSingle :: Core.Origin -> Core.SourceDict -> S.ByteString -> Either String Core.Point
decodeSingle o s y' =
  let
    ey = runGet decodeMessage y' :: Either String Protobuf.VaultPoint
  in
    case ey of
        Left err    -> Left err
        Right y     -> Right $ convertVaultToPoint o s y

{-
    Contents
-}

convertVaultToSource :: Protobuf.VaultContent -> Core.SourceDict
convertVaultToSource x =
  let
    ss = getField $ Protobuf.source x          :: [Protobuf.SourceTag]
    as = map convertToMapEntry ss              :: [(ByteString,ByteString)]
  in
    Core.SourceDict $ Map.fromList as


convertToMapEntry :: Protobuf.SourceTag -> (ByteString,ByteString)
convertToMapEntry tag =
  let
    k = getField $ Protobuf.field tag
    v = getField $ Protobuf.value tag
  in
    (k,v)

