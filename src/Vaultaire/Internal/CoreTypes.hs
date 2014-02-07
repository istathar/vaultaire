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

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# OPTIONS -fno-warn-orphans #-}

module Vaultaire.Internal.CoreTypes
(
    Point(..),
    Timestamp,
    SourceDict(..),
    Value(..),
    toHex,
    Origin(..),
    Directory,
    getSourcesMap,
    insertIntoDirectory,
    hashSourceDict,
    Label(..)
)
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.Int (Int64)
import Data.List (intercalate)
import Data.Locator
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Serialize
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word64)
import Text.Printf

type Timestamp = Word64

data Point = Point {
    origin    :: !Origin,
    source    :: !SourceDict,
    timestamp :: !Timestamp,
    payload   :: !Value
} deriving (Eq)


data Value
    = Empty
    | Numeric !Int64
    | Measurement !Double
    | Textual !Text
    | Blob !ByteString
    deriving (Eq, Show)


instance Show Point where
    show x = intercalate "\n"
        [show $ origin x,
         show $ source x,
         show $ timestamp x,
         case payload x of
                Empty       -> ""
                Numeric n   ->  show n
                Textual t   ->  T.unpack t
                Measurement r -> show r
                Blob b'     -> toHex b']


newtype SourceDict = SourceDict {
    runSourceDict :: Map ByteString ByteString
} deriving (Eq, Ord)


instance Show SourceDict where
    show x =
        intercalate ",\n" ps
      where
        m  = runSourceDict x
        ss = Map.toList m

        ps = map (\(k,v) -> (S.unpack k) ++ ":" ++ (S.unpack v)) ss

toHex :: ByteString -> String
toHex x =
  let
    f = concat . map (printf "%02X") . B.unpack
  in
    "0x" ++ f x


--
--
--
newtype Origin = Origin ByteString deriving (Eq, Ord, Show)

type Directory = Map Origin (Map SourceDict ByteString)

getSourcesMap :: Directory -> Origin -> Map SourceDict ByteString
getSourcesMap d o =
    Map.findWithDefault Map.empty o d


insertIntoDirectory :: Directory -> Origin -> Set SourceDict -> Directory
insertIntoDirectory d o st =
  let
    known = getSourcesMap d o

    f :: Map SourceDict ByteString -> SourceDict -> Map SourceDict ByteString
    f acc s = Map.insert s (hashSourceDict s) acc

    sm = Set.foldl f known st
  in
    Map.insert o sm d


{-
getDictionary :: Directory -> Map Text Text
getDictionary s =
    underlying s

getHashBase62 :: SourceDict -> ByteString
getHashBase62 s =
    hashBase62 s
-}
--
--
-- | The source dictionary portion of the bucket label is formed as follows:
--
-- 1. Sources are in a Data.Map which is a sorted map, per Ord order.
-- 2. Map is serialized to bytes by __cereal__'s "Data.Serialize.encode"
-- 3. The bytes are hashed with SHA1
-- 4. The hash is converted to 27 digits of base62
--
hashSourceDict :: SourceDict -> ByteString
hashSourceDict s =
  let
    m' = encode s
  in
    hashStringToBase62 27 m'


instance Serialize SourceDict where
--  put :: a -> Put
    put x =
      let
        m = runSourceDict x
      in
        put m

--  get :: Get a
    get = do
        m <- get
        return $ SourceDict (m :: Map ByteString ByteString)


newtype Label = Label ByteString deriving (Eq, Ord, Show)
