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

module Vaultaire.Internal.CoreTypes
(
    Point(..),
    Timestamp,
    SourceDict(..),
{-
    getDictionary,
    getHashBase62,
-}
    Value(..),
    toHex,
    Origin,
    Contents,
    nullContents,
    getSourcesMap,
    insertIntoContents,
    Label(..)
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

type Timestamp = Word64

data Point = Point {
    origin    :: !ByteString,
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
    runSourceDict :: Map Text Text
} deriving (Eq, Ord)


instance Show SourceDict where
    show x =
        intercalate ",\n" ps
      where
        m  = runSourceDict x
        ss = Map.toList m

        ps = map (\(k,v) -> (T.unpack k) ++ ":" ++ (T.unpack v)) ss

toHex :: ByteString -> String
toHex x =
  let
    f = concat . map (printf "%02X") . B.unpack
  in
    "0x" ++ f x


--
--
--
type Origin = ByteString

newtype Contents = Contents {
    runContents :: Map Origin (Map SourceDict ByteString)
} deriving (Eq, Show)

nullContents :: Contents
nullContents = Contents $ Map.empty

getSourcesMap :: Contents -> Origin -> Map SourceDict ByteString
getSourcesMap c o' =
  let
    om = runContents c
    sm = Map.findWithDefault Map.empty o' om
  in
    sm

insertIntoContents :: Contents -> Origin -> Set SourceDict -> Contents
insertIntoContents c o' st =
  let
    om = runContents c
    sm1 = Map.findWithDefault Map.empty o' om


    f :: Map SourceDict ByteString -> SourceDict -> Map SourceDict ByteString
    f acc s = Map.insert s "FIXME hash" acc

    sm2 = Set.foldl f sm1 st
  in
    Contents (Map.insert o' sm2 om)


{-
getDictionary :: Contents -> Map Text Text
getDictionary s =
    underlying s

getHashBase62 :: SourceDict -> ByteString
getHashBase62 s =
    hashBase62 s
-}

newtype Label = Label {
    runLabel :: ByteString
} deriving (Eq, Ord, Show)

