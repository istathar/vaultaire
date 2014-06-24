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

{-# LANGUAGE OverloadedStrings #-}
module Vaultaire.Types.ContentsOperation
(
    ContentsOperation(..),
    SourceDict,
) where

import Control.Applicative ((<$>), (<*>))
import qualified Data.ByteString as S
import Data.Packer (getBytes, getWord64LE, getWord8, putBytes, putWord64LE,
                    putWord8, runPacking, tryUnpacking)
import Vaultaire.Classes.WireFormat
import Vaultaire.Types.Address
import Vaultaire.Types.SourceDict (SourceDict)

data ContentsOperation = ContentsListRequest
                       | GenerateNewAddress
                       | UpdateSourceTag Address SourceDict
                       | RemoveSourceTag Address SourceDict
  deriving (Show, Eq)

instance WireFormat ContentsOperation where
    fromWire bs = flip tryUnpacking bs $ do
        header <- getWord8
        case header of
            0x0 -> return ContentsListRequest
            0x1 -> return GenerateNewAddress
            0x2 -> UpdateSourceTag <$> getAddr <*> getSourceDict
            0x3 -> RemoveSourceTag <$> getAddr <*> getSourceDict
            _   -> fail "Illegal op code"
      where
        getAddr = Address <$> getWord64LE
        getSourceDict = do
            len <- fromIntegral <$> getWord64LE
            fromWire <$> getBytes len >>= either (fail . show) return

    toWire op =
        case op of
            ContentsListRequest   -> "\x00"
            GenerateNewAddress    -> "\x01"
            UpdateSourceTag addr dict -> sourceOpToWire 0x2 addr dict
            RemoveSourceTag addr dict -> sourceOpToWire 0x3 addr dict
      where
        sourceOpToWire header (Address addr) dict =
            let dict_bytes = toWire dict in
            let dict_len = S.length dict_bytes in
            runPacking (17 + dict_len) $ do
                putWord8 header
                putWord64LE addr
                putWord64LE (fromIntegral dict_len)
                putBytes dict_bytes
