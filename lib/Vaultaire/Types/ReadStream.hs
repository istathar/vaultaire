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

import Control.Exception (SomeException (..))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Vaultaire.Classes.WireFormat

data ReadStream = InvalidReadOrigin
                | SimpleBurst { unSimpleBurst :: ByteString }
                | ExtendedBurst { unExtendedBurst :: ByteString }
                | EndOfStream
  deriving (Show, Eq)

instance WireFormat ReadStream where
    fromWire bs
        | bs == "\x00" = Right InvalidReadOrigin
        | bs == "\x01" = Right EndOfStream
        | BS.take 1 bs == "\x02" = Right $ SimpleBurst $ BS.drop 1 bs
        | BS.take 1 bs == "\x03" = Right $ ExtendedBurst $ BS.drop 1 bs
        | otherwise = Left $ SomeException $ userError "Invalid ReadStream packet"
    toWire InvalidReadOrigin = "\x00"
    toWire EndOfStream = "\x01"
    toWire (SimpleBurst bs) = "\x02" `BS.append` bs
    toWire (ExtendedBurst bs) = "\x03" `BS.append` bs
