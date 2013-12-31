--
-- Data vault for metrics
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module Main where

import Codec.Compression.LZ4
import Control.Applicative
import Control.Monad (forever)
import qualified Data.ByteString as S
import Data.List.NonEmpty (fromList)
import Data.Maybe (fromJust)
import System.ZMQ3.Monadic

import Vaultaire.Conversion.Receiver

main = runZMQ $ do
    pull <- socket Pull
    connect pull "tcp://10.42.149.3:5561"

    acks <- socket Push
    connect acks "tcp://10.42.149.3:5560"

    forever $ do
        [envelope', delimiter', message'] <- receiveMulti pull

        let burst' = fromJust $ decompress message'

        let eps = decodeBurst burst'

        liftIO $ case eps of
            Left err    -> print err
            Right ps    -> print ps

        liftIO $ putStrLn "----"

--
-- this works because we are manually following the rules of DEALER/ROUTER sockets
-- which sends along (one or more) "envelope" messages in a multipart so that the
-- downstream knows where to send acknowledgements to.
--

        sendMulti acks (fromList [envelope', delimiter', S.empty])

