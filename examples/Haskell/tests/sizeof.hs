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

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
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
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.Monoid (Monoid, mempty)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Data.Typeable (Typeable)
import Data.TypeLevel (D1, D2, D3, D4, D5, D6, D7, D8)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)

data DataFrame = DataFrame {
    source           :: Repeated D1 (Message Tag),
    timestamp        :: Required D2 (Value Word64),
    payload          :: Required D3 (Enumeration ValueType),
    valueNumeric     :: Optional D4 (Value Int64),
    valueMeasurement :: Optional D5 (Value Double),
    valueTextual     :: Optional D6 (Value Text),
    valueBlob        :: Optional D7 (Value ByteString)
} deriving (Generic, Eq)

instance Encode DataFrame
instance Decode DataFrame

instance Show DataFrame where
    show x =
        "sources:\n\t" ++ s ++ "\n" ++
        "timestamp:\n\t" ++ t ++ "\n" ++
        "payload:\n\t" ++ p ++ "\n" ++
        "value:\n\t" ++ v
      where
        s = intercalate ";\n\t" $ map show (getField $ source x)
        t = show $ getField $ timestamp x
        p = show $ getField $ payload x
        e = getField $ payload x

        v = case e of
                EMPTY   -> "EMPTY"
                NUMBER  ->  show $ fromMaybe 0 $ getField $ valueNumeric x
                TEXT    ->  T.unpack $ fromMaybe "" $ getField $ valueTextual x
                REAL    ->  show $ fromMaybe 0.0 $ getField $ valueMeasurement x
                BINARY  ->  show $ fromMaybe S.empty $ getField $ valueBlob x




data Tag = Tag {
    field :: Required D1 (Value Text),
    value :: Required D2 (Value Text)
} deriving (Generic, Eq)

instance Encode Tag
instance Decode Tag

instance Show Tag where
    show x =
        k ++ "," ++ v
      where
        k = T.unpack $ getField $ field x
        v = T.unpack $ getField $ value x


data ValueType
    = EMPTY
    | NUMBER
    | REAL
    | TEXT
    | BINARY
  deriving (Enum, Generic, Show, Eq)

instance Encode ValueType
instance Decode ValueType


main = do
    let tags =
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

    let msg = DataFrame {
        source = putField tags,
        timestamp = putField 1384727136000000000,
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

