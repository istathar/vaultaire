{-# LANGUAGE MultiParamTypeClasses #-}
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

-- | Marquise server library, for transmission of queued data to the vault.
module Marquise.Server
(
    sendNextBurst,
    marquiseServer
) where

import Control.Concurrent (threadDelay)
import Control.Exception (throwIO)
import Control.Monad (forever, unless)
import qualified Data.ByteString.Char8 as BS
import Marquise.Client (makeNameSpace)
import Marquise.IO (MarquiseServerMonad (..), spoolDir)
import Marquise.Types (NameSpace (..))
import System.Directory (doesDirectoryExist)
import Vaultaire.Types (Origin (..))

-- | Send the next burst, returns when the burst is acknowledged and thus in
-- the vault.
sendNextBurst :: MarquiseServerMonad m bp
              => String -> Origin -> NameSpace -> m ()
sendNextBurst broker origin ns = do
    maybe_burst <- nextBurst ns
    case maybe_burst of
        Nothing ->
            return ()
        Just (bp, bytes) -> do
            transmitBytes broker origin bytes
            flagSent bp

marquiseServer :: String -> String -> String -> IO ()
marquiseServer broker origin user_ns = do
    spool_exists <- doesDirectoryExist spoolDir
    unless spool_exists $ throwIO $ userError $
        "spool directory does not exist: " ++ spoolDir
    case makeNameSpace user_ns of
        Left e -> throwIO $ userError e
        Right ns -> forever $ do
            sendNextBurst broker (Origin $ BS.pack origin) ns
            threadDelay idleTime

idleTime :: Int
idleTime = 1000000 -- 1 second
