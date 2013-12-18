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

module WireFormat
(
    DataFrame(..), SourceTag(..), ValueType(..)
) where

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Debug.Trace

--
-- What we're using
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
    source           :: Repeated D1 (Message SourceTag),
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
        s ++ "\n" ++
        t ++ "\n" ++
        p ++ "\n" ++
        v
      where
        s = intercalate ",\n" $ map show (getField $ source x)
        t = show $ getField $ timestamp x
        p = show $ getField $ payload x
        e = getField $ payload x

        v = case e of
                EMPTY   -> ""
                NUMBER  ->  show $ fromMaybe 0 $ getField $ valueNumeric x
                TEXT    ->  T.unpack $ fromMaybe "" $ getField $ valueTextual x
                REAL    ->  show $ fromMaybe 0.0 $ getField $ valueMeasurement x
                BINARY  ->  show $ fromMaybe S.empty $ getField $ valueBlob x


data SourceTag = SourceTag {
    field :: Required D1 (Value Text),
    value :: Required D2 (Value Text)
} deriving (Generic, Eq)

instance Encode SourceTag
instance Decode SourceTag

instance Show SourceTag where
    show x =
        k ++ ":" ++ v
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


data DataBurst = DataBurst {
    frames :: Repeated D1 (Message DataFrame)
} deriving (Generic, Eq, Show)

instance Encode DataBurst
instance Decode DataBurst

