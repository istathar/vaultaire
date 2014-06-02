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

module Vaultaire.Types.ReadStream
(
    ReadStream(..),
) where

import Data.ByteString(ByteString)
import Vaultaire.Classes.WireFormat
import Control.Exception(SomeException(..))
import qualified Data.ByteString as BS

data ReadStream = InvalidReadOrigin
                | Burst { unBurst :: ByteString }
                | EndOfStream
  deriving (Show, Eq)

instance WireFormat ReadStream where
    fromWire bs
        | bs == "\x00" = Right InvalidReadOrigin
        | bs == "\x01" = Right EndOfStream
        | BS.take 1 bs == "\x02" = Right $ Burst $ BS.drop 1 bs
        | otherwise = Left $ SomeException $ userError "Invalid ReadStream packet"
    toWire InvalidReadOrigin = "\x00"
    toWire EndOfStream = "\x01"
    toWire (Burst bs) = "\x02" `BS.append` bs
