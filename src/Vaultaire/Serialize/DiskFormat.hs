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
{-# LANGUAGE DoAndIfThenElse    #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE StandaloneDeriving #-}

module Vaultaire.Serialize.DiskFormat
(
    Compression(..),
    Quantity(..),
    VaultPrefix(..),
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
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Data.TypeLevel (D1, D2, D3, D4, D5, D6, D7, D8)
import Data.Word
import Data.Word (Word32, Word64)
import GHC.Generics (Generic)
import Numeric (showIntAtBase)
import Prelude hiding (and, or)

import Vaultaire.Serialize.Common

and :: Bits a => a -> a -> a
and = (.&.)

or :: Bits a => a -> a -> a
or  = (.|.)

--
-- Framing used to indicate size of disk blocks of data is VaultPrefix. Note
-- that it is *not* a Protobuf; we use cereal directly.
--

data Compression = Normal | Compressed
  deriving (Show, Eq)

data Quantity = Single | Multiple
  deriving (Show, Eq)

data VaultPrefix = VaultPrefix {
    version     :: Word8,
    compression :: Compression,
    quantity    :: Quantity,
    size        :: Word64
} deriving (Show, Eq)


--
-- e v v v c m s s
-- 0 0 0 1 0 0 0 1  version 1, uncompressed, single item, 2 bytes size
--

instance Serialize VaultPrefix where

--  get :: Get VaultPrefix
    get = do
        b <- getWord8

        let w = b .&. 0x03       -- number of size bytes, 2^w
        s <- case w of
                    0x00    -> getWord8    >>= (return . fromIntegral)
                    0x01    -> getWord16le >>= (return . fromIntegral)
                    0x02    -> getWord32le >>= (return . fromIntegral)
                    0x03    -> getWord64le
                    _       -> error "Illegal width"

        let c = case (b .&. 0x08) of
                    0x0     -> Normal
                    0x8     -> Compressed
                    _       -> error "Illegal compression"

        let q = case (b .&. 0x04) of
                    0x0     -> Single
                    0x4     -> Multiple
                    _       -> error "Illegal quantity"

        let v = (b .&. 0x70) `shiftR` 4     -- version bits

        let e = b .&. 0x80                  -- extension bit

        return $ VaultPrefix {
                    version = v,
                    compression = c,
                    quantity    = q,
                    size = s
                }


--  put :: Putter VaultPrefix
    put x = do
        let e = if version x > 7
            then error "Unimplemented for version > 7"
            else 0x00

        let v = (version x) `shiftL` 4

        let c = case compression x of
                    Normal      -> 0x00
                    Compressed  -> 0x08

        let q = case quantity x of
                    Single      -> 0x00
                    Multiple    -> 0x04

        let w | size x < 256        = 0x00
              | size x < 65536      = 0x01
              | size x < 4294967296 = 0x02
              | otherwise           = 0x03

        let b = e .|. v .|. c .|. q .|. w

        putWord8 b

        case w of
            0x00    -> putWord8 (fromIntegral $ size x)
            0x01    -> putWord16le (fromIntegral $ size x)
            0x02    -> putWord32le (fromIntegral $ size x)
            0x03    -> putWord64le (fromIntegral $ size x)
            _       -> error "Illegal width"



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
