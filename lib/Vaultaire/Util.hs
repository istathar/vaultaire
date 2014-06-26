module Vaultaire.Util
(
    linkThread,
    waitForever,
    fatal
) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Monad
import Control.Monad.IO.Class
import System.IO.Unsafe (unsafePerformIO)
import System.Log.Logger

linkThread :: MonadIO m => IO a -> m ()
linkThread = liftIO . (async >=> link)

waitForever :: MonadIO m => m ()
waitForever = liftIO (threadDelay maxBound) >> waitForever

fatal :: String -> String -> a
fatal component err =
    unsafePerformIO (criticalM component err) `seq`
        error ("fatal error in " ++ component ++ ": " ++ err)
