--
-- Data vault for metrics
--
-- Copyright © 2014      Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE OverloadedStrings  #-}

module Vaultaire.JournalFile where
(
    BlockName,
    BlockSize,
    parseInboundJournal,
    makeInboundJournal
)

import Blaze.ByteString.Builder
import Blaze.ByteString.Builder.Char8

import qualified Data.ByteString.Char8 as S


--newtype BlockName = BlockName ByteString
type BlockName = ByteString
type BlockSize = Int


parseInboundJournal :: ByteString -> [(BlockName, BlockSize)]
parseInboundJournal = map f . S.lines
  where
    f l = case S.split ',' l of
            [a,b] -> (a,read b)
            _     -> error $ "Failed to parse journal file on line:\n\t" ++ l
            
makeInboundJournal :: [(BlockName, BlockSize)] -> ByteString
makeInboundJournal = toByteString . foldr f mempty
  where
    f builder (name, size) = builder <>
                             fromByteString name <>
                             fromChar ',' <>
                             fromShow size


