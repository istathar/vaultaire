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
{-# LANGUAGE RecordWildCards #-}

module DaemonRunners
(
   runBrokerDaemon,
   runWriterDaemon,
   runReaderDaemon,
   runContentsDaemon,
   runMarquiseDaemon
)
where

import qualified Control.Concurrent.Async as A
import Control.Concurrent.MVar
import qualified Data.ByteString.Char8 as S
import Data.Word (Word64)
import Pipes
import System.Log.Handler.Syslog
import System.Log.Logger
import System.ZMQ4.Monadic

import Marquise.Client
import Marquise.Server (marquiseServer)
import Vaultaire.Broker
import Vaultaire.Contents
import Vaultaire.Reader (startReader)
import Vaultaire.Writer (startWriter)


forkThread :: IO a -> IO ()
forkThread a = A.link =<< A.async a

forkThreadZMQ :: forall a z. ZMQ z a -> ZMQ z ()
forkThreadZMQ a = (liftIO . A.link) =<< async a

runBrokerDaemon :: MVar () -> IO ()
runBrokerDaemon _ = runZMQ $ do
    -- Writer proxy.
    forkThreadZMQ $ startProxy (Router,"tcp://*:5560")
                              (Dealer,"tcp://*:5561")
                              "tcp://*:5000"

    -- Reader proxy.
    forkThreadZMQ $ startProxy (Router,"tcp://*:5570")
                              (Dealer,"tcp://*:5571")
                              "tcp://*:5001"

    -- Contents proxy.
    forkThreadZMQ $ startProxy (Router,"tcp://*:5580")
                              (Dealer,"tcp://*:5581")
                              "tcp://*:5002"

    liftIO $ debugM "Main.runBroker" "Proxies started"

runReaderDaemon :: String -> String -> String -> MVar () -> IO ()
runReaderDaemon pool user broker shutdown =
    forkThread $ startReader ("tcp://" ++ broker ++ ":5571")
                (Just $ S.pack user)
                (S.pack pool)
                shutdown

runWriterDaemon :: String -> String -> String -> Word64 -> MVar () -> IO ()
runWriterDaemon pool user broker bucket_size shutdown =
    forkThread $ startWriter ("tcp://" ++ broker ++ ":5561")
                (Just $ S.pack user)
                (S.pack pool)
                bucket_size
                shutdown

runContentsDaemon :: String -> String -> String -> MVar () -> IO ()
runContentsDaemon pool user broker shutdown =
    forkThread $ startContents ("tcp://" ++ broker ++ ":5581")
                (Just $ S.pack user)
                (S.pack pool)
                shutdown

-- | Convenience wrapper around Marquise.Server
runMarquiseDaemon :: String -> Origin -> String -> MVar () -> IO ()
runMarquiseDaemon broker origin namespace _ = do
    forkThread $ marquiseServer broker origin namespace

