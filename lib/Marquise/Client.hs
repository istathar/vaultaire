{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
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

-- | Marquise client interface for sending data to the vault.
--
-- This module provides functions for preparing and queuing points to be sent
-- by a Marquise server to the vault.
--
-- If you call close, you can be assured that your data is safe and will at
-- some point end up in the data vault (excluiding local disk failure). This
-- assumption is based on a functional marquise daemon with connectivity
-- eventually running within your namespace.
-- 
-- We provide no way to *absolutely* ensure that a point is currently written
-- to the vault. Such a guarantee would require blocking and complex queuing,
-- or observing various underlying mechanisms that should ideally remain
-- abstract.
--
module Marquise.Client
(
    -- * Functions
    sendSimple,
    sendExtended,
    flush,
    -- * Types
    NameSpace,
    Address,
) where

import Data.ByteString(ByteString)
import Data.String(IsString)
import Vaultaire.CoreTypes(Address(..))
import Data.Word(Word64)

-- | A NameSpace implies a certain amount of Marquise server-side state. This
-- state being the Marquise server's authentication and origin configuration.
newtype NameSpace = NameSpace ByteString
  deriving (Eq, Show, IsString)

-- | Time since epoch in nanoseconds. Internally a 'Word64'.
newtype TimeStamp = TimeStamp Word64
  deriving (Show, Eq, Num, Bounded)

-- | This class is for convenience of testing. It encapsulates all IO
-- interaction that this module does.
class Monad m => MarquiseMonad m where
    -- | This append does not imply that the given data is synced to disk, just
    -- that it is queued to do so.
    append :: NameSpace -> ByteString -> m ()
    -- | Close any open handles and flush all previously appended datum to disk
    close :: NameSpace -> m ()

-- | Send a "simple" data point. Interpretation of this point, e.g.
-- float/signed is up to you, but it must be sent in the form of a Word64.
sendSimple
    :: MarquiseMonad m
    => NameSpace
    -> Address
    -> TimeStamp
    -> Word64
    -> m ()
sendSimple = undefined

-- | Send an "extended" data point. Again, representation is up to you.
sendExtended
    :: MarquiseMonad m
    => NameSpace
    -> Address
    -> TimeStamp
    -> ByteString
    -> m ()
sendExtended = undefined

-- | Ensure that all sent points have hit the local disk.
flush
    :: MarquiseMonad m
    => NameSpace
    -> m ()
flush = undefined
