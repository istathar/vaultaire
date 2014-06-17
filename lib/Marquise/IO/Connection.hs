--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.Connection
(
    withConnection,
    send,
    recv,
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Exception
import Data.List.NonEmpty (fromList)
import System.ZMQ4 hiding (send)
import Vaultaire.Types

withConnection :: String -> (Socket Dealer -> IO a) -> IO a
withConnection broker f =
    withContext $ \ctx ->
    withSocket ctx Dealer $ \s -> do
        connect s broker
        f s

send :: WireFormat request
     => request
     -> Origin
     -> Socket Dealer
     -> IO ()
send request (Origin origin) sock =
    sendMulti sock (fromList [origin, toWire request])

recv :: WireFormat response
     => Socket Dealer
     -> IO (Either SomeException response)
recv sock = do
    winner <- race waitTimeout receiveMessage
    case winner of
        Left () ->
            -- This really needs to trigger a disconnect and reconnect of the
            -- socket, or an out of sequence message will likely be recieved at
            -- some point.
            error "Timeout, non-recoverable due to un-implemented reconnect"
        Right x -> return x
  where
    receiveMessage = do
        resp <- receiveMulti sock
        return $ case resp of
            [msg] -> fromWire msg
            _ -> Left $ SomeException $ userError "expected one msg"

waitTimeout :: IO a
waitTimeout = do
    threadDelay 60000000
    error "Timeout, non-recoverable due to un-implemented reconnect"
