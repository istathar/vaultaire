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

{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DoAndIfThenElse            #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Vaultaire.Serialize.DiskFormat
(
    Word3(..),
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
import Data.Int (Int64)
import Data.List (intercalate)
import Data.ProtocolBuffers hiding (field)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word
import GHC.Generics (Generic)
import Prelude hiding (and, or)

import Vaultaire.Serialize.Common

and :: Bits a => a -> a -> a
and = (.&.)
{-# INLINE and #-}

or :: Bits a => a -> a -> a
or  = (.|.)
{-# INLINE or #-}

--
-- Framing used to indicate size of disk blocks of data is VaultPrefix. Note
-- that it is *not* a Protobuf; we use cereal directly.
--

newtype Word3 = Word3 {
    runWord3 :: Word8
} deriving (Bits, Eq, Show, Num, Integral, Real, Enum, Ord)

instance Bounded Word3 where
    minBound = 0
    maxBound = 7


data Compression = Normal | Compressed
  deriving (Show, Eq)

data Quantity = Single | Multiple
  deriving (Show, Eq)

data VaultPrefix = VaultPrefix {
    extended    :: Bool,
    version     :: Word3,
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

        let w = b `and` 0x03       -- number of size bytes, 2^w
        s <- case w of
                    0x00    -> getWord8    >>= (return . fromIntegral)
                    0x01    -> getWord16le >>= (return . fromIntegral)
                    0x02    -> getWord32le >>= (return . fromIntegral)
                    0x03    -> getWord64le
                    _       -> error "Illegal width"

        let c = case (b `and` 0x08) of
                    0x0     -> Normal
                    0x8     -> Compressed
                    _       -> error "Illegal compression"

        let q = case (b `and` 0x04) of
                    0x0     -> Single
                    0x4     -> Multiple
                    _       -> error "Illegal quantity"

        let v = (b `and` 0x70) `shiftR` 4     -- version bits

        let e = case (b `and` 0x80) of        -- extended bit
                    0x80    -> True
                    0x00    -> False
                    _       -> error "Illegal extension bit found"


        return $ VaultPrefix {
                    extended = e,
                    version = Word3 v,
                    compression = c,
                    quantity    = q,
                    size = s
                }


--  put :: Putter VaultPrefix
    put x = do
        let e = if extended x
            then 0x80
            else 0x00

        let v = (version x) `shiftL` 4

        let c = case compression x of
                    Normal      -> 0x00
                    Compressed  -> 0x08

        let q = case quantity x of
                    Single      -> 0x00
                    Multiple    -> 0x04

        let w | size x <= 255        = 0x00     -- maxBound :: Word8
              | size x <= 65535      = 0x01     -- maxBound :: Word16
              | size x <= 4294967295 = 0x02     -- maxBound :: Word32
              | otherwise            = 0x03

        let b = e `or` (fromIntegral v) `or` c `or` q `or` w

        putWord8 b

        case w of
            0x00    -> putWord8 (fromIntegral $ size x)
            0x01    -> putWord16le (fromIntegral $ size x)
            0x02    -> putWord32le (fromIntegral $ size x)
            0x03    -> putWord64le (fromIntegral $ size x)
            _       -> error "Illegal width"



data VaultContent = VaultContent {
    source :: Repeated 1 (Message SourceTag)
} deriving (Generic, Eq)

instance Encode VaultContent

instance Decode VaultContent

instance Serialize VaultContent where
--  put :: a -> Put
    put x = encodeMessage x

--  get :: Get a
    get = decodeMessage


instance Show VaultContent where
    show x =
        s
      where
        s = intercalate ",\n" $ map show (getField $ source x)


data VaultPoint = VaultPoint {
    timestamp        :: Required 2 (Value (Fixed Word64)),
    payload          :: Required 3 (Enumeration ValueType),
    valueNumeric     :: Optional 4 (Value Int64),
    valueMeasurement :: Optional 5 (Value Double),
    valueTextual     :: Optional 6 (Value Text),
    valueBlob        :: Optional 7 (Value ByteString)
} deriving (Generic, Eq)

instance Encode VaultPoint
instance Decode VaultPoint

instance Serialize VaultPoint where
--  put :: a -> Put
    put x = encodeMessage x

--  get :: Get a
    get = decodeMessage


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

