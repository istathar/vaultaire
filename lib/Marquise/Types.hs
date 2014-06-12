--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# OPTIONS_HADDOCK hide, prune #-}

module Marquise.Types
(
    SpoolName(..),
    TimeStamp(..),
    SimpleBurst(..),
    ExtendedBurst(..),
    SimplePoint(..),
    ExtendedPoint(..),
) where

import Data.Word (Word64)
import Data.ByteString(ByteString)

-- | A NameSpace implies a certain amount of Marquise server-side state. This
-- state being the Marquise server's authentication and origin configuration.
newtype SpoolName = SpoolName String
  deriving (Eq, Show)

-- | Time since epoch in nanoseconds. Internally a 'Word64'.
newtype TimeStamp = TimeStamp Word64
  deriving (Show, Eq, Num, Bounded)

data SimpleBurst = SimpleBurst ByteString
data ExtendedBurst = ExtendedBurst ByteString

data SimplePoint = SimplePoint
data ExtendedPoint = ExtendedPoint
