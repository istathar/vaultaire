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

module Vaultaire.Serialize.Common
(
    SourceTag(..),
    ValueType(..)
) where


import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.ProtocolBuffers hiding (field)
import Data.TypeLevel (D1, D2)
import GHC.Generics (Generic)


data SourceTag = SourceTag {
    field :: Required D1 (Value ByteString),
    value :: Required D2 (Value ByteString)
} deriving (Generic, Eq)

instance Encode SourceTag
instance Decode SourceTag

instance Show SourceTag where
    show x =
        k ++ ":" ++ v
      where
        k = S.unpack $ getField $ field x
        v = S.unpack $ getField $ value x


data ValueType
    = EMPTY
    | NUMBER
    | REAL
    | TEXT
    | BINARY
  deriving (Enum, Generic, Show, Eq)

instance Encode ValueType
instance Decode ValueType
