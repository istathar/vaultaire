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

module Receiver where

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
import Data.Maybe (fromMaybe)
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Data.Typeable (Typeable)
import Data.Word (Word32, Word64)

import qualified CoreTypes as Core
import WireFormat

convertToProtobuf :: Core.Point -> DataFrame
convertToProtobuf _ =
 let
   tags =
           [Tag {
                field = putField "hostname",
                value = putField "secure.example.org"
            },
            Tag {
                field = putField "metric",
                value = putField "eth0-tx-bytes"
            },
            Tag {
                field = putField "datacenter",
                value = putField "lhr1"
            },
            Tag {
                field = putField "epoch",
                value = putField "1"
            }]
  in
    DataFrame {
        source = putField tags,
        timestamp = putField 1386931666289843586,
        payload = putField NUMBER,
        valueNumeric = putField (Just 45000),
        valueMeasurement = mempty,
        valueTextual = mempty,
        valueBlob = mempty
    }




