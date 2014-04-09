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

originLookup :: OriginMap a -> Origin -> Maybe a
originLookup m k = HashMap.lookup k m

originInsert :: OriginMap a -> Origin -> a -> OriginMap a
originInsert m k v = HashMap.insert k v m

emptyOriginMap :: OriginMap a
emptyOriginMap = HashMap.empty
