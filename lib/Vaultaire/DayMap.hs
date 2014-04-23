module Vaultaire.DayMap
(
    DayMap,
    NumBuckets,
    Epoch,
    Time,
    lookupEpoch,
    lookupNumBuckets,
    lookupBoth,
    loadDayMap
) where

import Data.Word(Word64)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Control.Applicative
import Data.Packer

type Epoch = Word64
type NumBuckets = Word64

type Time = Word64
type DayMap = Map Epoch NumBuckets

lookupEpoch :: Time -> DayMap -> Epoch
lookupEpoch = (fst .) . lookupBoth

lookupNumBuckets :: Time -> DayMap -> NumBuckets
lookupNumBuckets = (snd .) . lookupBoth

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


lookupBoth :: Time -> DayMap -> (Epoch, NumBuckets)
lookupBoth t dm = 
    let (left, middle, _) = Map.splitLookup t dm
    in case middle of
        Just m -> if Map.null left -- Corner case, leftmost entry
                    then (t, m)
                    else Map.findMax left
        Nothing -> Map.findMax left
 
-- Internal

mustLoadDayMap :: ByteString -> DayMap
mustLoadDayMap =
    Map.fromList . runUnpacking parse
  where
    parse = many $ (,) <$> getWord64LE <*> getWord64LE
