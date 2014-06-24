--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_HADDOCK hide, prune #-}

module Marquise.Types
(
    SpoolName(..),
    SpoolFiles(..),
    TimeStamp(..),
    SimplePoint(..),
    ExtendedPoint(..),
    InvalidSpoolName(..),
) where

import Control.Exception
import Data.ByteString (ByteString)
import Data.Typeable
import Data.Word (Word64)
import Vaultaire.Types

-- | A NameSpace implies a certain amount of Marquise server-side state. This
-- state being the Marquise server's authentication and origin configuration.
newtype SpoolName = SpoolName { unSpoolName :: String }
  deriving (Eq, Show)

data SpoolFiles = SpoolFiles { pointsSpoolFile   :: FilePath
                             , contentsSpoolFile :: FilePath }
  deriving (Eq, Show)

-- | Time since epoch in nanoseconds. Internally a 'Word64'.
newtype TimeStamp = TimeStamp Word64
  deriving (Show, Eq, Num, Bounded)

data SimplePoint = SimplePoint { simpleAddress :: Address
                               , simpleTime    :: Time
                               , simplePayload :: Word64 }
  deriving Show

data ExtendedPoint = ExtendedPoint { extendedAddress :: Address
                                   , extendedTime    :: Time
                                   , extendedPayload :: ByteString }
  deriving Show

data InvalidSpoolName = InvalidSpoolName
  deriving (Show, Typeable)

instance Exception InvalidSpoolName
