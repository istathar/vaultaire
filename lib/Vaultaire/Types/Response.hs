{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | This module exports the Response type, which wraps all replies from
-- daemons with a "state".
module Vaultaire.Types.Response(
    Response(..)
)
where

import Control.Exception (SomeException (..))
import Data.Bifunctor (bimap)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Vaultaire.Classes.WireFormat (WireFormat (..))

-- | An acknowledgement of a message recieved, this will be attempted to be
-- delivered back to the sender of a 'Message'
data Response w = Success           -- ^ Signifies to the client to not
                                    --   retransmit.
              | Response w          -- ^ A good response
              | Failure ByteString  -- ^ Only sent in response to an invalid
                                    --   message that will never be accepted.

instance WireFormat w => WireFormat (Response w) where
    toWire Success        = ""
    toWire (Failure msg)  = "\x01\x00\x00\x00\x00\x00\x00\x00" `BS.append` msg
    toWire (Response msg) = "\x02\x00\x00\x00\x00\x00\x00\x00" `BS.append` toWire msg

    fromWire bs
        | BS.null bs =
            Right Success
        | BS.length bs < 8 =
            Left $ SomeException $ userError "Not enough bytes"
        | BS.take 8 bs == "\x01\x00\x00\x00\x00\x00\x00\x00" =
            Right $ Failure $ BS.drop 8 bs
        | BS.take 8 bs == "\x02\x00\x00\x00\x00\x00\x00\x00" =
            bimap id Response $ fromWire bs
        | otherwise =
            Left $ SomeException $ userError "Unknown packet"
