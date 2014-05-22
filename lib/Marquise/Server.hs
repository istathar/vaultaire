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

-- | Marquise server library, for transmission of queued data to the vault.
module Marquise.Server
(
    sendNextBurst,
    marquiseServer
) where

import Vaultaire.CoreTypes(Origin(..))
import Marquise.Types(NameSpace(..)) 
import Control.Monad(forever)
import Control.Concurrent(threadDelay)
import Marquise.IO(MarquiseServerMonad(..))

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

marquiseServer :: String -> Origin -> NameSpace -> IO ()
marquiseServer broker origin ns = forever $ do
    sendNextBurst broker origin ns
    threadDelay idleTime

idleTime :: Int
idleTime = 1000000 -- 1 second
