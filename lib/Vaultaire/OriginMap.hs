module Vaultaire.OriginMap
(
    OriginMap,
    originLookup,
    originInsert,
    originDelete,
    emptyOriginMap,
) where

import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap

import Vaultaire.Types

type OriginMap = HashMap Origin

originLookup :: Origin -> OriginMap a -> Maybe a
originLookup = HashMap.lookup

-- TODO: Unbackwards this
originInsert :: Origin -> a -> OriginMap a -> OriginMap a
originInsert = HashMap.insert

originDelete :: Origin -> OriginMap a ->  OriginMap a
originDelete = HashMap.delete

emptyOriginMap :: OriginMap a
emptyOriginMap = HashMap.empty
