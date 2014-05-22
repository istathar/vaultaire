--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

-- | Marquise server library, for transmission of queued data to the vault.
module Marquise.Server
(
    sendNextBurst
) where

import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Vaultaire.CoreTypes(Address(..))
import Data.Word(Word64)
import Marquise.Types(NameSpace(..), TimeStamp(..))
import Marquise.IO(MarquiseMonad(..))
import Data.Packer(runPacking, putWord64LE, putBytes)
import Data.Char(isAlphaNum)

-- | Send the next burst, burst will be split and the remainder pushed back to
-- the underlying store after the burst size specified
sendNextBurst :: Word64 -> NameSpace -> m ()
sendNextBurst = undefined
