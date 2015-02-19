module Vaultaire.DayMap
(
    DayMap(..),
    NumBuckets,
    Epoch,
    lookupFirst,
    lookupRange,
    loadDayMap
) where

import Control.Applicative
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.Map as Map
import Data.Packer
import Vaultaire.Types

-- | Parses a DayMap. Simple corruption check of input is done by
--   checking that it is a multiple of two Word64s; Left is returned if
--   corruption is detected, or if the provided ByteString is empty.
loadDayMap :: ByteString -> Either String DayMap
loadDayMap bs
    | BS.null bs =
        Left "empty"
    | BS.length bs `rem` 16 /= 0 =
        Left $ "corrupt contents, should be multiple of 16, was: " ++
               show (BS.length bs) ++ " bytes."
    | otherwise =
        let loaded = mustLoadDayMap bs
            (first, _) = Map.findMin (unDayMap loaded)
        in if first == 0
            then Right loaded
            else Left "bad first entry, must start at zero."

-- | Finds the first entry in the provided 'DayMap' that's after the
--   provided 'TimeStamp'.
lookupFirst :: TimeStamp -> DayMap -> (Epoch, NumBuckets)
lookupFirst start dm = fst $ splitRemainder start dm

-- | Return first entry and the remainder that is later than the provided
--   'TimeStamp'.
splitRemainder :: TimeStamp -> DayMap -> ((Epoch, NumBuckets), DayMap)
splitRemainder (TimeStamp t) (DayMap m) =
    let (left, middle, right) = Map.splitLookup t m
        first = case middle of
            Just n -> if Map.null left -- Corner case, leftmost entry
                        then (t, n)
                        else Map.findMax left
            Nothing -> Map.findMax left
    in (first, DayMap right)

-- | Get the DayMap entries between two TimeStamps.
lookupRange :: TimeStamp -> TimeStamp -> DayMap -> [(Epoch, NumBuckets)]
lookupRange start (TimeStamp end) dm =
    let (first, DayMap remainder) = splitRemainder start dm
        (rest,_) = Map.split end remainder
    in first : Map.toList rest

-- Internal

-- | Unpack a ByteString consisting of one or more
--   ('Epoch','NumBuckets') pairs into a 'DayMap'. Will throw an
--   'OutOfBoundUnpacking' error on badly-formed data.
mustLoadDayMap :: ByteString -> DayMap
mustLoadDayMap =
    DayMap . Map.fromList . runUnpacking parse
  where
    parse = many $ (,) <$> getWord64LE <*> getWord64LE
