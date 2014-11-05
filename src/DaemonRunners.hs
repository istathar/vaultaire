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

--
-- | This module encapsulates the various daemons that you might want to start
-- up as part of a Vaultaire cluster, along with their default behaviours.
--
module DaemonRunners (
   DaemonProcess,
   waitDaemon,
   runBrokerDaemon,
   runWriterDaemon,
   runReaderDaemon,
   runContentsDaemon,
   forkThread
   ) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.MVar
import qualified Data.ByteString.Char8 as S
import Data.Unique
import Data.Maybe
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
import Vaultaire.Profiler (startProfiler)


-- have an option for forking a telemetry thread associated with a worker thread

type DaemonProcess a = ( Async a           -- worker thread
                       , Maybe (Async ())) -- profiler thread

waitDaemon :: DaemonProcess a -> IO a
waitDaemon (worker, Nothing)       =         wait     worker
waitDaemon (worker, Just profiler) = fst <$> waitBoth worker profiler

forkThread  :: IO a -> IO (Async a)
forkThread action = do
    a <- async action
    link a
    return a

forkThreads :: IO a -> Maybe (IO ()) -> IO (DaemonProcess a)
forkThreads action profiler = do
    a <- async action
    link a
    b <- maybe (return Nothing) (fmap Just . async) profiler
    return (a, b)

linkThreadZMQ :: forall a z. ZMQ z a -> ZMQ z ()
linkThreadZMQ a = (liftIO . link) =<< Z.async a

runBrokerDaemon :: MVar () -> IO (DaemonProcess ())
runBrokerDaemon end =
    flip forkThreads Nothing $ do
        infoM "Daemons.runBroker_uriDaemon" "Broker_uri daemon started"
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

runReaderDaemon :: String -> String -> URI -> MVar () -> Maybe Period
                -> IO (DaemonProcess ())
runReaderDaemon pool user broker_uri end profiling_period = do
    infoM "Daemons.runReaderDaemon" "Reader daemon starting"
    args <- daemonArgs ("tcp://" ++ broker_uri ++ ":5571")
                       pool user end
                       (const "reader" <$> profiling_period)
    forkThreads (startReader   args)
                (startProfiler args <$> profiling_period)

runWriterDaemon :: String -> String -> URI -> Word64 -> MVar () -> Maybe Period
                -> IO (DaemonProcess ())
runWriterDaemon pool user broker_uri bucket_size end profiling_period = do
    infoM "Daemons.runWriterDaemon" "Writer daemon starting"
    args <- daemonArgs ("tcp://" ++ broker_uri ++ ":5561")
                       pool user end
                       (const "writer" <$> profiling_period)
    forkThreads (startWriter args bucket_size)
                (startProfiler args <$> profiling_period)

runContentsDaemon :: String -> String -> URI -> MVar () -> Maybe Period
                  -> IO (DaemonProcess ())
runContentsDaemon pool user broker_uri end profiling_period = do
    infoM "Daemons.runContentsDaemon" "Contents daemon starting"
    args <- daemonArgs ("tcp://" ++ broker_uri ++ ":5581")
                       pool user end
                       (const "contents" <$> profiling_period)
    forkThreads (startContents args)
                (startProfiler args <$> profiling_period)

-- | Convient helper for creating daemon args.
daemonArgs :: URI -> String -> String -> MVar ()
           -> Maybe Name
           -> IO DaemonArgs
daemonArgs full_broker_uri pool user end mname = do
    prof  <- maybe (return Nothing)
                   (\s -> do uname <- uniqueDaemonName full_broker_uri s
                             conn  <- setupProfiling uname
                             return $ Just conn)
                   mname
    return $ DaemonArgs full_broker_uri
                        (Just $ S.pack user)
                        (S.pack pool)
                        end prof

-- Attempt to create a unique daemon name
-- FIXME: is this what we actually want?
uniqueDaemonName :: URI -> String -> IO Name
uniqueDaemonName broker_uri n = do
  u <- newUnique
  return $ concat [broker_uri, ":", n, "-", show $ hashUnique u]
