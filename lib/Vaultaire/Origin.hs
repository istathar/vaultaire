{-# LANGUAGE OverloadedStrings #-}

{-|
Low-level origin-related definitions which should not be exposed to
clients (and therefore don't belong in Vaultaire.Types).
-}
module Vaultaire.Origin
(
    namespaceOrigin,
    Namespace(..)
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS

import Vaultaire.Types

-- | Suffix appended to the origin segment of a bucket name to indicate
--   that it's for internal use.
internalNamespace :: ByteString
internalNamespace = "_INTERNAL"

-- | Convert a raw origin to the raw origin used as a bucket prefix. If
--   the desired operation is on an internal bucket, the origin is
--   qualified with the appropriate suffix; if not, it is returned
--   unmodified.
namespaceOrigin :: Namespace  -- ^ Is the operation internal or external?
                -> Origin     -- ^ Base origin (of the form returned by
                              --   'makeOrigin')
                -> Origin
namespaceOrigin Internal = Origin . (`BS.append` internalNamespace) . unOrigin
namespaceOrigin External = id

-- | An origin has both Internal and External buckets. Regular
--   simple and extended points written by clients go in External
--   buckets; Internal buckets are not directly accessible by clients
--   and are used under the hood to store metadata.
data Namespace = Internal | External
