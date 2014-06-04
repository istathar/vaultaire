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

import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.Types
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
    describe "Addresses" $ do
        it "encodes an address in base62" $ do
            encodeAddressToString 0 `shouldBe` "00000000000"
            encodeAddressToString (2^64-1) `shouldBe` "LygHa16AHYF"
            encodeAddressToString (minBound :: Address) `shouldBe` "00000000000"
            encodeAddressToString (maxBound :: Address) `shouldBe` "LygHa16AHYF"

        it "decodes an address from base62" $ do
            decodeStringAsAddress "00000000000" `shouldBe` 0
            decodeStringAsAddress "00000000001" `shouldBe` 1
            decodeStringAsAddress "LygHa16AHYF" `shouldBe` (2^64-1)
            decodeStringAsAddress "LygHa16AHYG" `shouldBe` 0

