{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Vaultaire.CoreTypes
(
    Address(..),
    calculateBucketNumber,
    isAddressExtended
) where

import Data.Bits
import Data.Word (Word64)

newtype Address = Address Word64
    deriving (Eq, Num)

--
-- | Which bucket does this address belong to?
--
calculateBucketNumber :: Word64 -> Address -> Word64
calculateBucketNumber num_buckets (Address addr) = (addr `clearBit` 0) `mod` num_buckets

isAddressExtended :: Address -> Bool
isAddressExtended (Address addr) = addr `testBit` 0
