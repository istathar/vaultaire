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

module Vaultaire.Internal.CoreTypes
(
    Point(..),
    Value(..),
    toHex,
    Contents(..)
)
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Int (Int64)
import Data.List (intercalate)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word64)
import Text.Printf


data Point = Point {
    origin    :: ByteString,
    source    :: Map Text Text,
    timestamp :: Word64,     -- ?
    payload   :: Value
} deriving (Eq)


data Value
    = Empty
    | Numeric Int64
    | Measurement Double
    | Textual Text
    | Blob ByteString
    deriving (Eq, Show)


instance Show Point where
    show x = intercalate "\n"
        [showSourceMap $ source x,
         show $ timestamp x,
         case payload x of
                Empty       -> ""
                Numeric n   ->  show n
                Textual t   ->  T.unpack t
                Measurement r -> show r
                Blob b'     -> toHex b']


showSourceMap m =
        intercalate ",\n" ps
      where
        ss = Map.toList m

        ps = map (\(k,v) -> (T.unpack k) ++ ":" ++ (T.unpack v)) ss


toHex :: ByteString -> String
toHex x =
  let
    f = concat . map (printf "%02X") . B.unpack
  in
    "0x" ++ f x


data Contents = Contents {
    locator :: ByteString,         -- origin
    sources :: Set (Map Text Text)
} deriving (Eq, Show)

