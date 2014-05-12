{-# LANGUAGE DeriveGeneric #-}

module Vaultaire.OriginMap
(
    Origin(..),
    OriginMap,
    originLookup,
    originInsert,
    originDelete,
    emptyOriginMap,
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Hashable
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.String
import GHC.Generics (Generic)

newtype Origin = Origin ByteString
    deriving (Eq, Ord, Generic)

instance Show Origin where
    show (Origin o') = S.unpack o'

instance Hashable Origin

instance IsString Origin where
--  fromString :: String -> Origin
    fromString x = Origin (S.pack x)

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
