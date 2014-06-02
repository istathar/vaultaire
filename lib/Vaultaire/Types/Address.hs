{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Vaultaire.Types.Address
(
    Address(..),
    encodeAddressToString,
    decodeStringAsAddress,
    calculateBucketNumber,
    isAddressExtended,
    Origin(..)
) where

import Data.Bits
import Data.ByteString (ByteString)
import Data.Hashable
import Data.Locator
import Data.Packer
import Data.String
import Data.Word (Word64)
import Data.Packer(runPacking, tryUnpacking, getWord64LE, putWord64LE)
import GHC.Generics (Generic)

import Vaultaire.Classes.WireFormat

newtype Address = Address {
    unAddress :: Word64
} deriving (Eq, Num, Bounded)

instance Show Address where
    show = encodeAddressToString

instance IsString Address where
    fromString = decodeStringAsAddress

-- | There are assumptions made that the encoding of Address is fixed-length (8
-- bytes). Changing that will break things subtly.
instance WireFormat Address where
    toWire = runPacking 8 . putWord64LE . unAddress
    fromWire = tryUnpacking (Address `fmap` getWord64LE)

encodeAddressToString :: Address -> String
encodeAddressToString = padWithZeros 11 . toBase62 . toInteger . unAddress

decodeStringAsAddress :: String -> Address
decodeStringAsAddress = fromIntegral . fromBase62

--
-- | Which bucket does this address belong to?
--
calculateBucketNumber :: Word64 -> Address -> Word64
calculateBucketNumber num_buckets (Address addr) = (addr `clearBit` 0) `mod` num_buckets

isAddressExtended :: Address -> Bool
isAddressExtended (Address addr) = addr `testBit` 0


newtype Origin = Origin { unOrigin :: ByteString }
    deriving (Eq, Ord, Generic, IsString, Hashable, Show)
