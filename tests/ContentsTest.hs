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

import ArbitraryInstances ()
import Control.Exception (throw)
import Data.HashMap.Strict (fromList)
import Data.Text
import Marquise.Client
import Pipes.Prelude (toListM)
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import TestHelpers
import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.Types
import Vaultaire.Util

startDaemons :: IO ()
startDaemons = do
    linkThread $ runZMQ $
        startProxy (Router,"tcp://*:5580") (Dealer,"tcp://*:5581") "tcp://*:5008"
    linkThread $ startContents "tcp://localhost:5581" Nothing "test"

main :: IO ()
main = do
    startDaemons
    hspec suite

suite :: Spec
suite = do
    -- TODO: This does not belong here, move to another test at the least.
    -- The reason for encodeAddressToString and decodeStringAsAddress beyond
    -- Show and IsString is questionable. Is this made use of anywhere? Perhaps
    -- we can remove it before we have to maintain it.
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

    describe "Full stack" $ do
        it "unions two dicts" $ do
            let dict_a = listToDict [("a", "1")]
            let dict_b = listToDict [("a", "2")]
            let addr = 1

            cleanupTestEnvironment

            xs <- withBroker "localhost" (Origin "PONY") $ do
                updateSourceDict addr dict_a >>= either throw return
                updateSourceDict addr dict_b >>= either throw return
                toListM enumerateOrigin
            case xs of
                [(addr', dict)] -> do
                    dict `shouldBe` dict_b
                    addr' `shouldBe` addr
                _ -> error "expected one"

        it "updates source dict for any address" $
            property propSourceDictUpdated


listToDict :: [(Text, Text)] -> SourceDict
listToDict elts = either error id . makeSourceDict $ fromList elts

propSourceDictUpdated :: Address -> SourceDict -> Property
propSourceDictUpdated addr dict = monadicIO $ do
    xs <- run $ do
        -- Clear out ceph
        cleanupTestEnvironment
        withBroker "localhost" (Origin "PONY") $ do
            updateSourceDict addr dict >>= either throw return
            toListM enumerateOrigin
    case xs of
        [(addr', dict')] -> assert (addr' == addr && dict' == dict)
        _ -> error "expected one"
