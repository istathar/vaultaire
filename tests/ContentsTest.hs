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

{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HashMap
import System.ZMQ4.Monadic hiding (Event)

import Test.Hspec hiding (pending)

import Vaultaire.Broker
import Vaultaire.ContentsEncoding
import Vaultaire.ContentsServer
import Vaultaire.Util


startBroker :: IO ()
startBroker = do
    linkThread $ runZMQ $
        startProxy (Router,"tcp://*:5580") (Dealer,"tcp://*:5581") "tcp://*:5008"
    linkThread $ startContents "tcp://localhost:5581" Nothing "test"

main :: IO ()
main = do
    startBroker
    hspec suite

suite :: Spec
suite = do
    describe "Contents Operations" $ do
       it "opcodes encode correctly" $ do
            opcodeToWord64 (ContentsListRequest 0) `shouldBe`   0x0
            opcodeToWord64 RegisterNewAddress `shouldBe`    0x1
            opcodeToWord64 (UpdateSourceTag HashMap.empty) `shouldBe`       0x2
            opcodeToWord64 (RemoveSourceTag HashMap.empty) `shouldBe`       0x3
