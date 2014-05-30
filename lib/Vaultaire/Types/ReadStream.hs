--
-- Data vault for metrics
--
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

module Vaultaire.Types.ReadStream
(
    ReadStream(..),
) where

import Data.ByteString(ByteString)
import Vaultaire.Classes.WireFormat

newtype ReadStream = Burst { unBurst :: ByteString }
  deriving (Show, Eq)

instance WireFormat ReadStream where
    fromWire = Right . Burst
    toWire = unBurst
