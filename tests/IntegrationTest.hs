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


import Test.Hspec hiding (pending)
import Control.Concurrent
import Data.HashMap.Strict (fromList)
import Data.Text
import Marquise.Client

import DaemonRunners

startDaemons :: IO ()
startDaemons = do
    shutdown <- newEmptyMVar
    runBrokerDaemon shutdown


main :: IO ()
main = do
    startDaemons
    hspec suite

suite :: Spec
suite = do
    describe "Things" $ do
        it "does stuff" $ do
            True `shouldBe` True


listToDict :: [(Text, Text)] -> SourceDict
listToDict elts = either error id . makeSourceDict $ fromList elts
