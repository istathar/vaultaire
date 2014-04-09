module Vaultaire.DayMap
(
    DayMap,
    NoBuckets,
    Epoch,
    Time,
    lookupEpoch,
    lookupNoBuckets,
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
type NoBuckets = Word64

type Time = Word64
type DayMap = Map Epoch NoBuckets

lookupEpoch :: DayMap -> Time -> Epoch
lookupEpoch = (fst .) . lookupGeneric

lookupNoBuckets :: DayMap -> Time -> NoBuckets
lookupNoBuckets = (snd .) . lookupGeneric

loadDayMap :: ByteString -> Either String DayMap
loadDayMap bs
    | BS.length bs `rem` 16 /= 0 = Left "corrupt"
    | otherwise = Right $ mustLoadDayMap bs

-- Internal

lookupGeneric :: DayMap -> Time -> (Epoch, NoBuckets)
lookupGeneric dm t = 
    let (left, middle, _) = Map.splitLookup t dm
    in case middle of
        Just m -> if Map.null left
                    then (t, m)
                    else Map.findMax left
        Nothing -> Map.findMax left
 
mustLoadDayMap :: ByteString -> DayMap
mustLoadDayMap =
    Map.fromList . runUnpacking (many ((,) <$> getWord64LE <*> getWord64LE))
