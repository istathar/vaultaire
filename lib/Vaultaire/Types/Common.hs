--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Vaultaire.Types.Common
(
    Origin(..),
    Epoch,
    NumBuckets,
    Time,
    DayMap
) where

import Data.ByteString (ByteString)
import Data.Hashable (Hashable)
import Data.Map (Map)
import Data.String (IsString)
import Data.Word (Word64)

newtype Origin = Origin { unOrigin :: ByteString }
    deriving (Eq, Ord, IsString, Hashable, Show)


-- These can all be newtype wrapped as make work, perhaps excluding DayMap.
-- They have no reason to be inter-mixed.

type Epoch = Word64
type NumBuckets = Word64

type Time = Word64
type DayMap = Map Epoch NumBuckets
