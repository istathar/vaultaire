module Vaultaire.OriginMap
(
    Origin,
    OriginMap,
    originLookup,
    originInsert,
    emptyOriginMap,
) where

import Data.ByteString(ByteString)
import Data.HashMap.Strict(HashMap)
import qualified Data.HashMap.Strict as HashMap

type Origin = ByteString
type OriginMap = HashMap Origin

originLookup :: Origin -> OriginMap a -> Maybe a
originLookup = HashMap.lookup 

originInsert :: OriginMap a -> Origin -> a -> OriginMap a
originInsert m k v = HashMap.insert k v m

emptyOriginMap :: OriginMap a
emptyOriginMap = HashMap.empty
