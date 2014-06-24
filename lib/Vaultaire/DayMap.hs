module Vaultaire.DayMap
(
    DayMap,
    NumBuckets,
    Epoch,
    Time,
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

-- | Simple corruption check of input is done by checking that it is a multiple
-- of two Word64s
loadDayMap :: ByteString -> Either String DayMap
loadDayMap bs
    | BS.null bs =
        Left "empty"
    | BS.length bs `rem` 16 /= 0 =
        Left $ "corrupt contents, should be multiple of 16, was: " ++
               show (BS.length bs) ++ " bytes."
    | otherwise =
        let loaded = mustLoadDayMap bs
            (first, _) = Map.findMin loaded
        in if first == 0
            then Right loaded
            else Left "bad first entry, must start at zero."


lookupFirst :: Time -> DayMap -> (Epoch, NumBuckets)
lookupFirst = (fst .) . splitRemainder

-- Return first and the remainder that is later than that.
splitRemainder :: Time -> DayMap -> ((Epoch, NumBuckets), DayMap)
splitRemainder t dm =
    let (left, middle, right) = Map.splitLookup t dm
        first = case middle of
            Just m -> if Map.null left -- Corner case, leftmost entry
                        then (t, m)
                        else Map.findMax left
            Nothing -> Map.findMax left
    in (first, right)

lookupRange :: Time -> Time -> DayMap -> [(Epoch, NumBuckets)]
lookupRange start end dm =
    let (first, remainder) = splitRemainder start dm
        (rest,_) = Map.split end remainder
    in first : Map.toList rest

-- Internal

mustLoadDayMap :: ByteString -> DayMap
mustLoadDayMap =
    Map.fromList . runUnpacking parse
  where
    parse = many $ (,) <$> getWord64LE <*> getWord64LE
