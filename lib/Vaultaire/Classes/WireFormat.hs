module Vaultaire.Classes.WireFormat
(
    WireFormat(..),
) where

import Control.Exception (SomeException)
import Data.ByteString (ByteString)

-- | This typeclass encapsulates all wire encoding/decoding, with the
-- possibility of a decode failing.
class WireFormat operation where
    fromWire :: ByteString -> Either SomeException operation
    toWire   :: operation -> ByteString

-- | Default dummy wire format
instance WireFormat () where
    fromWire = error "() has no wire format"
    toWire = error "() has no wire format"
