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
