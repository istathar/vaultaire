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
        | S.take 1 bs == "\x02" = ContentsListEntry <$> fromWire (S.drop 1 bs)
                                                    <*> fromWire (S.drop 9 bs)
        | otherwise = Left $ SomeException $
                        userError "Invalid ContentsResponse packet"

    toWire InvalidContentsOrigin = "\x00"
    toWire (RandomAddress addr)  = "\x01" `S.append` toWire addr
    toWire (ContentsListEntry addr dict) =
        "\x02" `S.append` toWire addr `S.append` toWire dict

--  Be aware there is also case such that:
--  toWire (ContentsListBypass addr b) =
--      "\x02" ...
--  so that raw encoded bytes stored on disk can be tunnelled through. See
--  Vaultaire.Types.ContentsListBypass for details

    toWire EndOfContentsList = "\x03"
    toWire UpdateSuccess     = "\x04"
    toWire RemoveSuccess     = "\x05"
