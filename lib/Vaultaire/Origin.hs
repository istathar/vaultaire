{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Origin
(
    namespaceOrigin
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS

import Vaultaire.Types

-- | Suffix appended to the origin segment of a bucket name to indicate
--   that it's for internal use, e.g., for metadata buckets read/written
--   by the contents daemon.
internalNamespace :: ByteString
internalNamespace = "_INTERNAL"

-- | Convert a raw origin to the raw origin used as a bucket prefix. If
--   the desired operation is on an internal bucket, the origin is
--   qualified with the appropriate suffix; if not, it is returned
--   unmodified.
namespaceOrigin :: Bool   -- ^ Is the operation internal?
                -> Origin
                -> Origin
namespaceOrigin True = Origin . (`BS.append` internalNamespace) . unOrigin
namespaceOrigin False = id
