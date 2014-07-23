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

{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where


import Control.Concurrent
import Data.HashMap.Strict (fromList)
import Data.Text
import Pipes
import qualified Pipes.Prelude as P
import Test.Hspec hiding (pending)

import DaemonRunners
import Marquise.Client

startDaemons :: IO ()
startDaemons =
  let
    pool   = "vaultaire"
    user   = "vaultaire"
    broker = "localhost"
    bucket_size = 4194304
    origin = Origin "ZZZZZZ"
    namespace = "integration"
  in do
    shutdown <- newEmptyMVar
    runBrokerDaemon shutdown
    runWriterDaemon pool user broker bucket_size shutdown
    runReaderDaemon pool user broker shutdown
    runContentsDaemon pool user broker shutdown
    runMarquiseDaemon broker origin namespace shutdown

setupClientSide :: IO SpoolFiles
setupClientSide = do
    createSpoolFiles "integration"

main :: IO ()
main = do
    startDaemons
    spool <- setupClientSide
    hspec (suite spool)

suite :: SpoolFiles -> Spec
suite spool =
  let
    origin    = Origin "ZZZZZZ"
    address   = hashIdentifier "Row row row yer boat"
    begin     = 1406078299651575183
    end       = 1406078299651575183
    timestamp = 1406078299651575183
    payload   = 42
  in do
    describe "Generate data" $ do
        it "sends point via marquise" $ do
            queueSimple spool address timestamp payload
            flush spool
            pass

    describe "Retreive data" $ do
        it "reads point via marquise" $ do
            result <- withReaderConnection "localhost" $ \c -> do
                P.head (readSimple address begin end origin c >-> decodeSimple)

            case result of
                Nothing -> expectationFailure "Expected a value back, didn't get one"
                Just v  -> (simplePayload v) `shouldBe` payload


-- | Mark that we are expecting this code to have succeeded, unless it threw an exception
pass :: Expectation
pass = return ()

listToDict :: [(Text, Text)] -> SourceDict
listToDict elts = either error id . makeSourceDict $ fromList elts

