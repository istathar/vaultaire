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

module Vaultaire.Serialize.DiskFormat
(
    VaultHeader(..),
    VaultContent(..),
    VaultPoint(..),
    SourceTag(..),
    ValueType(..)
) where

import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Char (intToDigit)
import Data.Int (Int64)
import Data.List (intercalate)
import Data.ProtocolBuffers hiding (field)
import Data.Text (Text)
import qualified Data.Text as T
import Data.TypeLevel (D1, D2, D3, D4, D5, D6, D7, D8)
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)
import Numeric (showIntAtBase)

import Vaultaire.Serialize.Common


data VaultContent = VaultContent {
    origin  :: Required D8 (Value ByteString),
    source  :: Repeated D1 (Message SourceTag),
    payload :: Required D3 (Enumeration ValueType)
} deriving (Generic, Eq)

instance Encode VaultContent

instance Decode VaultContent

instance Show VaultContent where
    show x =
        o ++ "\n" ++
        s ++ "\n" ++
        p
      where
        o = S.unpack $ (getField $ origin x)
        s = intercalate ",\n" $ map show (getField $ source x)
        p = show $ getField $ payload x


data VaultHeader = VaultHeader {
    header :: Required D1 (Value (Fixed Word32))
} deriving (Generic, Eq)

instance Encode VaultHeader

instance Decode VaultHeader

instance Show VaultHeader where
    show x =
      let
        (Fixed raw) = getField $ header x
        flags = raw `shiftR` 24
        size = raw .&. 16777215

        f = showIntAtBase 2 intToDigit flags ""
        n = show size
      in
        "Flags: " ++ f ++
        "Bytes: " ++ n


data VaultPoint = VaultPoint {
    timestamp        :: Required D2 (Value (Fixed Word64)),
    valueNumeric     :: Optional D4 (Value Int64),
    valueMeasurement :: Optional D5 (Value Double),
    valueTextual     :: Optional D6 (Value Text),
    valueBlob        :: Optional D7 (Value ByteString)
} deriving (Generic, Eq)

instance Encode VaultPoint
instance Decode VaultPoint

instance Show VaultPoint where
    show x =
        ms ++
        vn ++
        vt ++
        vr ++
        vb
      where
        (Fixed m) = getField $ timestamp x
        ms = (show m) ++ "\n"

        vn = case getField $ valueNumeric x of
                Nothing -> ""
                Just n  -> (show n) ++ "\n"
        vt = case getField $ valueTextual x of
                Nothing -> ""
                Just t  -> (T.unpack t) ++ "\n"
        vr = case getField $ valueMeasurement x of
                Nothing -> ""
                Just r  -> (show r) ++ "\n"
        vb = case getField $ valueBlob x of
                Nothing -> ""
                Just b' -> (S.unpack b') ++ "\n"
