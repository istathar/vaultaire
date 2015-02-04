{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Origin
(
    namespaceOrigin
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS

import Vaultaire.Types

internalNamespace :: ByteString
internalNamespace = "_INTERNAL"

internalOrigin :: Origin -> Origin
internalOrigin = Origin . (`BS.append` internalNamespace) . unOrigin

-- | Convert a raw origin to the raw origin used as a bucket prefix. If
--   the desired operation is on an internal bucket, the origin is
--   qualified with the appropriate suffix; if not, it is returned
--   unmodified.
namespaceOrigin :: Bool   -- ^ Is the operation internal?
                -> Origin
                -> Origin
namespaceOrigin True = internalOrigin
namespaceOrigin False = id
