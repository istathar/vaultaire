{-# LANGUAGE OverloadedStrings #-}
--
-- Data vault for metrics
--
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

module Vaultaire.Types.WriteResult
(
    WriteResult(..),
) where

import Control.Exception (SomeException (..))
import Vaultaire.Classes.WireFormat

data WriteResult = InvalidWriteOrigin | OnDisk
  deriving (Show, Eq)

instance WireFormat WriteResult where
    fromWire bs
        | bs == "\x00" = Right OnDisk
        | bs == "\x01" = Right InvalidWriteOrigin
        | otherwise = Left $ SomeException $ userError "Invalid WriteResult packet"
    toWire OnDisk = "\x00"
    toWire InvalidWriteOrigin = "\x01"
