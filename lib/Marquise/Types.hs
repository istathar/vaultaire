{-# LANGUAGE GeneralizedNewtypeDeriving #-}
--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

module Marquise.Types
(
    NameSpace(..),
    TimeStamp(..),
) where

import Data.Word(Word64)

-- | A NameSpace implies a certain amount of Marquise server-side state. This
-- state being the Marquise server's authentication and origin configuration.
newtype NameSpace = NameSpace String
  deriving (Eq, Show)

-- | Time since epoch in nanoseconds. Internally a 'Word64'.
newtype TimeStamp = TimeStamp Word64
  deriving (Show, Eq, Num, Bounded)
 

