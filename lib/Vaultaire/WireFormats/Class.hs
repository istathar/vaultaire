module Vaultaire.WireFormats.Class
(
    WireFormat(..),
) where

import Data.ByteString(ByteString)
import Control.Exception(SomeException)

-- | This typeclass encapsulates all wire encoding/decoding, with the
-- possibility of a decode failing.
class WireFormat operation where
    fromWire :: ByteString -> Either SomeException operation
    toWire   :: operation -> ByteString
