module Vaultaire.Util
(
    linkThread,
    waitForever
) where

import Control.Concurrent.Async
import Control.Concurrent
import Control.Monad.IO.Class

linkThread :: MonadIO m => IO a -> m ()
linkThread a = liftIO (async a >>= link)

waitForever :: MonadIO m => m ()
waitForever = liftIO (threadDelay maxBound) >> waitForever
