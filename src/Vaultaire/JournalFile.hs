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

module Vaultaire.JournalFile
(
    BlockName,
    BlockSize,
    parseInboundJournal,
    makeInboundJournal
) where

import Blaze.ByteString.Builder
import Blaze.ByteString.Builder.Char8
import Data.ByteString (ByteString)
import Data.Monoid((<>), mempty)
import Data.List(foldl')

import qualified Data.ByteString.Char8 as S


--newtype BlockName = BlockName ByteString
type BlockName = ByteString
type BlockSize = Int


parseInboundJournal :: ByteString -> [(BlockName, BlockSize)]
parseInboundJournal = map f . S.lines
  where
    f l = case S.split ',' l of
            [a,b] -> case S.readInteger b of
                Just (n,_)  -> (a, fromIntegral n)
                Nothing -> die l
            _ -> die l
    die l = error $ "Failed to parse size in journal file on line:\n\t" ++ S.unpack l
            
makeInboundJournal :: [(BlockName, BlockSize)] -> ByteString
makeInboundJournal = toByteString . foldl' f mempty
  where
    f builder (name, size) = builder <>
                             fromByteString name <>
                             fromChar ',' <>
                             fromShow size <>
                             fromChar '\n'


