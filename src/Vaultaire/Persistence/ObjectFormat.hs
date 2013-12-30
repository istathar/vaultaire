--
-- Data vault for metrics
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Persistence.ObjectFormat (
    formObjectLabel
) where

import qualified Data.ByteString.Char8 as S
import Data.Word

import qualified Vaultaire.Internal.CoreTypes as Core

--
-- Number of seconds per bucket
--

__WINDOW_SIZE__ :: Int
__WINDOW_SIZE__ = 100000


{-
import qualified Data.Map.Strict as Map

data Bucket = Bucket {
    origin :: Text,
    source2 :: Map Text Text,
    timemark :: Word64      -- seconds since epoch, div 100000
} deriving (Eq, Show)
-}

{-
    I'd really like to think there's an easier way of doing constants
-}

windowSize :: Word64
windowSize = fromIntegral __WINDOW_SIZE__

nanoseconds :: Word64
nanoseconds = fromIntegral $ (1000000000 :: Int)

--
-- Use the relevant information from a point to find out what bucket
-- it belongs in.
--
formObjectLabel :: Core.Point -> S.ByteString
formObjectLabel p =
    S.intercalate "_" [o', s', t']
  where
    o' = Core.origin p
    s' = "ABCD"
    t  = (Core.timestamp p) `div` (windowSize * nanoseconds)
    t' = S.pack $ show (t * windowSize)

-- HERE hashable? TODO FIXME

