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

{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE TupleSections   #-}

--
-- | This module encapsulates the various daemons that you might want to start
-- up as part of a Vaultaire cluster, along with their default behaviours.
--
module DaemonRunners (
   DaemonProcess,
   waitDaemon,
   forkThread,
   forkThreads,
   daemonWorker,
   daemonProfiler,
   runBrokerDaemon,
   runWriterDaemon,
   runReaderDaemon,
   runContentsDaemon
   ) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Data.Maybe
import Network.URI
import Pipes
import System.Log.Logger
import System.ZMQ4.Monadic hiding (async)
import qualified System.ZMQ4.Monadic as Z

import Vaultaire.Daemon
import Vaultaire.Broker
import Vaultaire.Contents (startContents)
import Vaultaire.Reader   (startReader)
import Vaultaire.Writer   (startWriter)
import Vaultaire.Profiler
import Vaultaire.Util


type DaemonProcess a = ( Async a           -- worker thread
                       , Maybe (Async ())) -- profiler thread

daemonWorker :: DaemonProcess a -> Async a
daemonWorker = fst

daemonProfiler :: DaemonProcess a -> Maybe (Async ())
daemonProfiler = snd

-- | Wait for a worker daemon, and its profiler - if any, to finish.
waitDaemon :: DaemonProcess a -> IO a
waitDaemon (worker, Nothing)   =         wait     worker
waitDaemon (worker, Just prof) = fst <$> waitBoth worker prof

-- | Fork a worker daemon thread.
forkThread  :: IO a -> IO (Async a)
forkThread action = do
    a <- async action
    link a
    return a

-- | Fork a daemon worker thread and (maybe) a profiler thread associated with it.
forkThreads :: IO a -> Maybe (IO ()) -> IO (DaemonProcess a)
forkThreads action prof = do
    a <- async action
    link a
    b <- maybe (return Nothing) (fmap Just . async) prof
    _ <- maybe (return ())      (link2 a)           b

    return (a, b)

linkThreadZMQ :: forall a z. ZMQ z a -> ZMQ z ()
linkThreadZMQ a = (liftIO . link) =<< Z.async a

runBrokerDaemon :: MVar () -> IO (DaemonProcess ())
runBrokerDaemon end =
    flip forkThreads Nothing $ do
        infoM "Daemons.runBrokerDaemon" "Broker daemon started"
        runZMQ $ do
            -- Writer proxy.
            linkThreadZMQ $ startProxy
                (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"

            -- Reader proxy.
            linkThreadZMQ $ startProxy
                (Router,"tcp://*:5570") (Dealer,"tcp://*:5571") "tcp://*:5001"

            -- Contents proxy.
            linkThreadZMQ $ startProxy
                (Router,"tcp://*:5580") (Dealer,"tcp://*:5581") "tcp://*:5002"

            -- Telemetry proxy.
            linkThreadZMQ $ startProxy
                (XSub,  "tcp://*:6660") (XPub,"tcp://*:6661") "tcp://*:6000"

        readMVar end

runWorkerDaemon
    :: String                -- ^ Ceph pool
    -> String                -- ^ Ceph user
    -> String                -- ^ Broker URI
    -> MVar ()               -- ^ Shutdown
    -> String                -- ^ Optional daemon name
    -> Maybe (Period,Int)    -- ^ Optional profiler (period, channel bound)
    -> (DaemonArgs -> IO ()) -- ^ Run this worker daemon
    -> IO (DaemonProcess ())
runWorkerDaemon pool user brok down name prof daemon = do
    (args, env) <- daemonArgs (fromMaybe (fatal "runWorkerDaemon" "Invalid broker URI")
                                         (parseURI brok))
                              (Just user) pool down
                              (if name == "" then Nothing else Just name)
                              (trip profilingPort <$> prof)
    forkThreads (daemon args)
                (fmap (const $ startProfiler env) prof)
    where trip x (y,z) = (x,y,z)

runWriterDaemon pool user brok rollover down name prof = do
    infoM "Daemons.runWriterDaemon" "Writer daemon starting"
    runWorkerDaemon pool user ("tcp://" ++ brok ++ ":5561")
                    down name prof (flip startWriter rollover)

runReaderDaemon pool user brok down name prof = do
    infoM "Daemons.runReaderDaemon" "Reader daemon starting"
    runWorkerDaemon pool user ("tcp://" ++ brok ++ ":5571")
                    down name prof startReader

runContentsDaemon pool user brok down name prof = do
    infoM "Daemons.runContentsDaemon" "Contents daemon starting"
    runWorkerDaemon pool user ("tcp://" ++ brok ++ ":5581")
                    down name prof startContents

-- The other ports are hard-coded here, so we define the profiling port here too.
profilingPort = 6661
