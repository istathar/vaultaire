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

{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.ContentsServer
(
    startContents,
    Operation(..),
    -- testing
    opcodeToWord64
) where

import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word64)
import Control.Monad (forever)

import Vaultaire.Daemon
import Vaultaire.ContentsEncoding

--
-- Daemon implementation
--

data Operation =
    ContentsListRequest |
    RegisterNewAddress |
    UpdateSourceTag |
    RemoveSourceTag
  deriving
    (Show, Eq, Enum)



-- | Start a writer daemon, never returns.
startContents
    :: String           -- ^ Broker
    -> Maybe ByteString -- ^ Username for Ceph
    -> ByteString       -- ^ Pool name for Ceph
    -> IO ()
startContents broker user pool =
    runDaemon broker user pool $ forever $ nextMessage >>= handleRequest

handleRequest :: Message -> Daemon ()
handleRequest (Message reply payload origin) =
    undefined


opcodeToWord64 :: Operation -> Word64
opcodeToWord64 op = fromIntegral $ fromEnum op
