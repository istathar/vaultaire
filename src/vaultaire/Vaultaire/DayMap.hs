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

lookupEpoch :: Time -> DayMap -> Epoch
lookupEpoch = (fst .) . lookupGeneric

lookupNoBuckets :: Time -> DayMap -> NoBuckets
lookupNoBuckets = (snd .) . lookupGeneric

-- | Simple corruption check of input is done by checking that it is a multiple
-- of two Word64s
loadDayMap :: ByteString -> Either String DayMap
loadDayMap bs
    | BS.length bs `rem` 16 /= 0 = Left "corrupt"
    | otherwise = Right $ mustLoadDayMap bs

-- Internal

lookupGeneric :: Time -> DayMap -> (Epoch, NoBuckets)
lookupGeneric t dm = 
    let (left, middle, _) = Map.splitLookup t dm
    in case middle of
        Just m -> if Map.null left -- Corner case, leftmost entry
                    then (t, m)
                    else Map.findMax left
        Nothing -> Map.findMax left
 
mustLoadDayMap :: ByteString -> DayMap
mustLoadDayMap =
    Map.fromList . runUnpacking parse
  where
    parse = many $ (,) <$> getWord64LE <*> getWord64LE
