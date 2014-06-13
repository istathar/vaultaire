--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

module Vaultaire.Types.PassThrough
(
    PassThrough(..)
) where

import Vaultaire.Classes.WireFormat
import Data.ByteString(ByteString)

newtype PassThrough = PassThrough { unPassThrough :: ByteString }
  deriving (Eq, Show)

instance WireFormat PassThrough where
    toWire = unPassThrough
    fromWire = Right . PassThrough
