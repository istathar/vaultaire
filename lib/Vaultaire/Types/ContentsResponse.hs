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

module Vaultaire.Types.ContentsResponse
(
    ContentsResponse(..),
) where

import Control.Applicative ((<$>), (<*>))
import Control.Exception (SomeException (..))
import qualified Data.ByteString as S
import Vaultaire.Classes.WireFormat
import Vaultaire.Types.Address
import Vaultaire.Types.SourceDict
import Data.Packer(runPacking, runUnpacking, putWord8, putWord64LE, putBytes, getBytes, getWord64LE)

data ContentsResponse = RandomAddress Address
                      | InvalidContentsOrigin
                      | ContentsListEntry Address SourceDict
                      | EndOfContentsList
                      | UpdateSuccess
                      | RemoveSuccess
  deriving (Show, Eq)


instance WireFormat ContentsResponse where
    fromWire bs
        | bs == "\x00" = Right InvalidContentsOrigin
        | bs == "\x03" = Right EndOfContentsList
        | bs == "\x04" = Right UpdateSuccess
        | bs == "\x05" = Right RemoveSuccess
        | S.take 1 bs == "\x01" = RandomAddress <$> fromWire (S.drop 1 bs)
        -- This relies on address being fixed-length when encoded
        | S.take 1 bs == "\x02" = do
            let body = S.drop 1 bs
            let unpacker = (,) <$> getBytes 8 <*> (getWord64LE >>= getBytes . fromIntegral)
            let (addr_bytes, dict_bytes ) = flip runUnpacking body unpacker
            ContentsListEntry <$> fromWire addr_bytes <*> fromWire dict_bytes

        | otherwise = Left $ SomeException $
                        userError "Invalid ContentsResponse packet"

    toWire InvalidContentsOrigin = "\x00"
    toWire (RandomAddress addr)  = "\x01" `S.append` toWire addr
    toWire (ContentsListEntry addr dict) =
        let addr_bytes = toWire addr
            dict_bytes = toWire dict
            dict_len = S.length dict_bytes
        in runPacking (dict_len + 17) $ do
            putWord8 0x2
            putBytes addr_bytes
            putWord64LE $ fromIntegral dict_len
            putBytes dict_bytes

--  Be aware there is also case such that:
--  toWire (ContentsListBypass addr b) =
--      "\x02" ...
--  so that raw encoded bytes stored on disk can be tunnelled through. See
--  Vaultaire.Types.ContentsListBypass for details

    toWire EndOfContentsList = "\x03"
    toWire UpdateSuccess     = "\x04"
    toWire RemoveSuccess     = "\x05"
