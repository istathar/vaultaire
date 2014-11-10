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
import Data.Word (Word64)
import Pipes
import System.Log.Logger
import System.ZMQ4.Monadic hiding (async)
import qualified System.ZMQ4.Monadic as Z

import Vaultaire.Types
import Vaultaire.Daemon
import Vaultaire.Broker
import Vaultaire.Contents (startContents)
import Vaultaire.Reader   (startReader)
import Vaultaire.Writer   (startWriter)
import Vaultaire.Profiler


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
    -- Do not link the worker with the profiler thread,
    -- as the worker should not die if the profiler does.
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
                (Router,"tcp://*:5590") (Dealer,"tcp://*:5591") "tcp://*:5003"

        readMVar end

runReaderDaemon :: String -> String -> URI -> MVar ()
                -> Maybe ProfilerArgs
                -> IO (DaemonProcess ())
runReaderDaemon pool user broker_uri end profiling = do
    infoM "Daemons.runReaderDaemon" "Reader daemon starting"
    args <- daemonArgs ("tcp://" ++ broker_uri ++ ":5571")
                       (Just user)
                       pool
                       end
                       profiling
    forkThreads (startReader   args)
                (if   isJust profiling
                 then Just $ startProfiler args
                 else Nothing)

runWriterDaemon :: String -> String -> URI -> Word64 -> MVar ()
                -> Maybe ProfilerArgs
                -> IO (DaemonProcess ())
runWriterDaemon pool user broker_uri bucket_size end profiling = do
    infoM "Daemons.runWriterDaemon" "Writer daemon starting"
    args <- daemonArgs ("tcp://" ++ broker_uri ++ ":5561")
                       (Just user)
                       pool
                       end
                       profiling
    forkThreads (startWriter   args bucket_size)
                (if   isJust profiling
                 then Just $ startProfiler args
                 else Nothing)

runContentsDaemon :: String -> String -> URI -> MVar ()
                  -> Maybe ProfilerArgs
                  -> IO (DaemonProcess ())
runContentsDaemon pool user broker_uri end profiling = do
    infoM "Daemons.runContentsDaemon" "Contents daemon starting"
    args <- daemonArgs ("tcp://" ++ broker_uri ++ ":5581")
                       (Just user)
                       pool
                       end
                       profiling
    forkThreads (startContents args)
                (if   isJust profiling
                 then startProfiler args
                 else Nothing)
