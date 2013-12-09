--
-- Experimental snippet
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module Main where

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Debug.Trace

--
-- What we're testing
--

import Data.Hex
import Data.Int (Int64)
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers
import Data.Serialize
import Data.Text (Text)
import Data.Typeable (Typeable)
import Data.TypeLevel (D1, D2, D3, D4, D5, D6, D7, D8)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)

data DataFrame = DataFrame {
    source           :: Repeated D1 (Message Tag),
    timestamp        :: Required D2 (Value Word64),
    payload          :: Required D3 (Enumeration ValueType),
    valueNumeric     :: Optional D4 (Value Int64),
    valueMeasurement :: Optional D6 (Value Double),
    valueTextual     :: Optional D5 (Value Text),
    valueBlob        :: Optional D6 (Value ByteString)
} deriving (Generic, Show, Eq)

instance Encode DataFrame
instance Decode DataFrame


data Tag = Tag {
    key             :: Required D1 (Value Text),
    value           :: Required D2 (Value Text)
} deriving (Generic, Show, Eq)

instance Encode Tag
instance Decode Tag


data ValueType
    = EMPTY
    | NUMBER
    | TEXT
    | REAL
  deriving (Enum, Generic, Show, Eq)

instance Encode ValueType
instance Decode ValueType


main = do
    let tags =
           [Tag {
                key = putField "hostname",
                value = putField "secure.example.org"
            },
            Tag {
                key = putField "metric",
                value = putField "eth0-tx-bytes"
            },
            Tag {
                key = putField "datacenter",
                value = putField "lhr1"
            },
            Tag {
                key = putField "epoch",
                value = putField "1"
            }]

    let msg = DataFrame {
        source = putField tags,
        timestamp = putField 1384727136,
        payload = putField NUMBER,
        valueNumeric = putField (Just 45000),
        valueMeasurement = mempty,
        valueTextual = mempty,
        valueBlob = mempty
    }

    putStrLn ""
    putStrLn $ show msg
    putStrLn ""

    let x' = runPut $ encodeMessage msg

    S.putStrLn $ hex x'
    putStrLn ""
    putStr "Length: "
    putStrLn $ show $ S.length x'

    putStrLn ""

    let e = runGet decodeMessage x' :: Either String DataFrame
    
    case e of
        Left err    -> do
            putStrLn $ show err

        Right msg2   -> do
            putStrLn $ show msg2
            putStrLn ""

            if msg == msg2
                then putStrLn "Same"
                else error "Different"

