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
import System.Environment (getArgs, getProgName)
import System.ZMQ4.Monadic
import Text.Groom
import System.Rados

import Vaultaire.Internal.CoreTypes
import Vaultaire.Conversion.Receiver
import Vaultaire.Persistence.BucketObject

validateBurst :: [Point] -> IO ()
validateBurst [] = return ()
validateBurst (p:_) =
  let
    o' = origin p
  in
    if S.null o'
        then error "Empty origin value, discarding burst"
        else return ()


processBurst :: [Point] -> IO ()
processBurst [] = return ()
processBurst ps = do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            mapM_ appendVaultPoint ps


main :: IO ()
main = do
    args <- getArgs

    let broker = if length args == 1
                    then head args
                    else error "Specify broker hostname or IP address on command line"

    runZMQ $ do
        pull <- socket Pull
        connect pull ("tcp://" ++ broker ++ ":5561")

        ack  <- socket Push
        connect ack  ("tcp://" ++ broker ++ ":5560")

        forever $ do
            [envelope', delimiter', message'] <- receiveMulti pull

            let burst' = fromJust $ decompress message'

            let eps = decodeBurst burst'

            liftIO $ case eps of
                Left err    -> putStrLn err
                Right ps    -> do
                    validateBurst ps
                    processBurst ps
                    putStrLn $ show $ length ps -- FIXME

--
-- We have to use sendMulti because we are manually following the rules of
-- DEALER/ROUTER sockets which send along (one or more) "envelope" messages in
-- a multipart so that the downstream knows where to send acknowledgements to; you
-- return the
--

            sendMulti ack (fromList [envelope', delimiter', S.empty])

