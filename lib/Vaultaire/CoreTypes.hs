{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Vaultaire.CoreTypes
(
    Address(..),
    calculateBucketNumber,
    isAddressExtended,
    Origin(..)
) where

import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Hashable
import Data.String
import Data.Word (Word64)
import GHC.Generics (Generic)

newtype Address = Address {
    unAddress :: Word64
} deriving (Show, Eq, Num, Bounded)

--
-- | Which bucket does this address belong to?
--
calculateBucketNumber :: Word64 -> Address -> Word64
calculateBucketNumber num_buckets (Address addr) = (addr `clearBit` 0) `mod` num_buckets

isAddressExtended :: Address -> Bool
isAddressExtended (Address addr) = addr `testBit` 0


newtype Origin = Origin ByteString
    deriving (Eq, Ord, Generic)

instance Show Origin where
    show (Origin o') = S.unpack o'

instance Hashable Origin

instance IsString Origin where
--  fromString :: String -> Origin
    fromString x = Origin (S.pack x)
