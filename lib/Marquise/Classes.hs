--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}

module Marquise.Classes
(
    MarquiseWriterMonad(..),
    MarquiseSpoolFileMonad(..),
    MarquiseReaderMonad(..),
    MarquiseContentsMonad(..),
) where

import Control.Exception (SomeException)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LB
import Marquise.Types
import Vaultaire.Types

-- | This class is for convenience of testing. It encapsulates all IO
-- interaction that the client and server will do.
class Monad m => MarquiseSpoolFileMonad m where
    randomSpoolFiles :: SpoolName -> m SpoolFiles

    createDirectories :: SpoolName -> m ()

    -- | Append to the spool file for points, i.e. data.
    --
    -- This append does not imply that the given data is synced to disk, just
    -- that it is queued to do so. This assumes no state, so any file handles
    -- must be stashed globally or re-opened and closed.
    appendPoints :: SpoolFiles -> ByteString -> m ()

    -- | Append  to the spool file for contents updates, i.e. metadata.
    appendContents :: SpoolFiles -> ByteString -> m ()

    -- | Return an lazy bytestring and an IO action to signify that the burst
    -- has been completely sent.
    --
    -- May block until something is actually spooled up.
    nextPoints :: SpoolName -> m (LB.ByteString, m ())
    nextContents :: SpoolName -> m (LB.ByteString, m ())

    -- | Close any open handles and flush all previously appended datum to disk
    close :: SpoolFiles -> m ()

class Monad m => MarquiseWriterMonad m where
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
