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

import System.ZMQ4.Monadic hiding (Event)

import Test.Hspec hiding (pending)

import Control.Concurrent
import Data.HashMap.Strict (fromList)
import Data.String
import Data.Text
import Marquise.Client
import Pipes.Prelude (toListM)
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import TestHelpers
import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.Util

startDaemons :: IO ()
startDaemons = do
    shutdown <- newEmptyMVar
    linkThread $ do
        runZMQ $ startProxy (Router,"tcp://*:5580")
                            (Dealer,"tcp://*:5581") "tcp://*:5008"
        readMVar shutdown

    linkThread $ startContents "tcp://localhost:5581" Nothing "test" shutdown

main :: IO ()
main = do
    startDaemons
    hspec suite

suite :: Spec
suite = do
    describe "Things" $ do
        it "does stuff" $ do
            return True


listToDict :: [(Text, Text)] -> SourceDict
listToDict elts = either error id . makeSourceDict $ fromList elts
