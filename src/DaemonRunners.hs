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
module DaemonRunners
(
   forkThread,
   runBrokerDaemon,
   runWriterDaemon,
   runReaderDaemon,
   runContentsDaemon
)
where

import Control.Concurrent.Async
import Control.Concurrent.MVar
import qualified Data.ByteString.Char8 as S
import Data.Unique
import Data.Word (Word64)
import Pipes
import System.Log.Logger
import System.ZMQ4.Monadic hiding (async)
import qualified System.ZMQ4.Monadic as Z

import Vaultaire.Types
import Vaultaire.Daemon
import Vaultaire.Broker
import Vaultaire.Contents (startContents)
import Vaultaire.Reader (startReader)
import Vaultaire.Writer (startWriter)


-- have an option for forking a telemetry thread associated with a worker thread

forkThread :: IO a -> IO (Async a)
forkThread action = do
    a <- async action
    link a
    return a


linkThreadZMQ :: forall a z. ZMQ z a -> ZMQ z ()
linkThreadZMQ a = (liftIO . link) =<< Z.async a

runBrokerDaemon :: MVar () -> IO (Async ())
runBrokerDaemon shutdown_signal =
    forkThread $ do
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

        readMVar shutdown_signal

runReaderDaemon :: URI -> String -> String -> MVar () -> IO (Async ())
runReaderDaemon broker_uri user pool shutdown_signal =
    forkThread $ do
        infoM "Daemons.runReaderDaemon" "Reader daemon started"
        uname <- uniqueDaemonName broker_uri "reader"
        startReader $ DaemonArgs ("tcp://" ++ broker_uri ++ ":5571")
                                 uname
                                 (Just $ S.pack user)
                                 (S.pack pool)
                                 shutdown_signal

runWriterDaemon :: String -> String -> String -> Word64 -> MVar () -> IO (Async ())
runWriterDaemon pool user broker_uri bucket_size shutdown_signal =
    forkThread $ do
        infoM "Daemons.runWriterDaemon" "Writer daemon started"
        uname <- uniqueDaemonName broker_uri "writer"
        startWriter (DaemonArgs ("tcp://" ++ broker_uri ++ ":5561")
                                uname
                                (Just $ S.pack user)
                                (S.pack pool)
                                shutdown_signal)
                     bucket_size

runContentsDaemon :: String -> String -> String -> MVar () -> IO (Async ())
runContentsDaemon pool user broker_uri shutdown_signal =
    forkThread $ do
        infoM "Daemons.runContentsDaemon" "Contents daemon started"
        uname <- uniqueDaemonName broker_uri "contents"
        startContents $ DaemonArgs ("tcp://" ++ broker_uri ++ ":5581")
                                   uname
                                   (Just $ S.pack user)
                                   (S.pack pool)
                                   shutdown_signal

-- Attempt to create a unique daemon name
-- FIXME: is this what we actually want?
uniqueDaemonName :: URI -> String -> IO Name
uniqueDaemonName broker_uri n = do
  u <- newUnique
  return $ concat [broker_uri, ":", n, "-", show $ hashUnique u]
