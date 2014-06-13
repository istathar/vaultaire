--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}

module Marquise.Classes
(
    MarquiseWriterMonad(..),
    MarquiseSpoolFileMonad(..),
    MarquiseReaderMonad(..),
    MarquiseContentsMonad(..),
) where

import qualified Data.ByteString.Lazy as LB
import Marquise.Types
import Vaultaire.Types
import Data.ByteString(ByteString)
import Control.Exception(SomeException)

-- | This class is for convenience of testing. It encapsulates all IO
-- interaction that the client and server will do.
class Monad m => MarquiseSpoolFileMonad m where
    -- | This append does not imply that the given data is synced to disk, just
    -- that it is queued to do so. This assumes no state, so any file handles
    -- must be stashed globally or re-opened and closed.
    append :: SpoolName -> LB.ByteString -> m ()
    -- | Close any open handles and flush all previously appended datum to disk
    close :: SpoolName -> m ()

class MarquiseSpoolFileMonad m => MarquiseWriterMonad m bp | m -> bp where
    -- | Atomically empty the underlying store and retrieve the next "burst" of
    -- appended datums. Appended datums are *not* separated. They're all
    -- concatenated together into the same ByteString.
    nextBurst :: SpoolName -> m (Maybe (bp, ByteString))
    -- | Clean up a sent burst. This should be called on a successfull ack.
    flagSent :: bp -> m ()

    -- | Send bytes upstream, returns when ack recieved.
    transmitBytes :: String      -- ^ Broker address
                  -> Origin      -- ^ Origin
                  -> ByteString  -- ^ Bytes to send
                  -> m ()

class Monad m => MarquiseContentsMonad m connection | m -> connection where
    withContentsConnection :: String -> (connection -> m a) -> m a
    sendContentsRequest    :: ContentsOperation -> Origin -> connection -> m ()
    recvContentsResponse   :: connection -> m (Either SomeException ContentsResponse)

class Monad m => MarquiseReaderMonad m connection | m -> connection where
    withReaderConnection :: String -> (connection -> m a) -> m a
    sendReaderRequest    :: ReadRequest -> Origin -> connection -> m ()
    recvReaderResponse   :: connection -> m (Either SomeException ReadStream)
