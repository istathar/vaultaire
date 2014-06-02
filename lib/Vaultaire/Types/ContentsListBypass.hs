{-# LANGUAGE OverloadedStrings #-}
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | This module provides a ContentsListBypass type which is required to
-- somewhat subvert the type system we use to ensure that anything passed to
-- 'reply' is a valid WireFormat instance.
--
-- In the case of reading contents lists from Ceph, we already have a valid
-- wireformat bytestring. It is just missing the Address. So given an (Address,
-- ByteString) tuple, we can transform it one-way into a ContentsListBypass in
-- such a way that:
--
-- forall SourceDict s.
-- forall Address a.
-- toWire (ContentsListBypass (a, toWire s)) == toWire (ContentsListEntry a s)
-- 
-- The point of this is to avoid re-parsing on egress from disk.
module Vaultaire.Types.ContentsListBypass
(
    ContentsListBypass(..)
) where

import qualified Data.ByteString as S
import Data.ByteString(ByteString)
import Vaultaire.Types.Address
import Vaultaire.Classes.WireFormat

data ContentsListBypass = ContentsListBypass Address ByteString

instance WireFormat ContentsListBypass where
    fromWire = error "fromWire for ContentsListBypass is not implemented"
    toWire (ContentsListBypass a s) = 
        "\x02" `S.append` toWire a `S.append` s
