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

import System.ZMQ4.Monadic

import Test.Hspec hiding (pending)

import Control.Concurrent
import Data.HashMap.Strict (fromList)
import Data.Maybe
import Data.String
import Data.Text
import Network.URI
import Pipes.Prelude (toListM)
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)

import Marquise.Client
import TestHelpers
import Vaultaire.Broker
import Vaultaire.Contents
import Vaultaire.Daemon
import Vaultaire.Util

startDaemons :: IO ()
startDaemons = do
    quit <- newEmptyMVar
    linkThread $ do
        runZMQ $ startProxy (Router,"tcp://*:5580")
                            (Dealer,"tcp://*:5581") "tcp://*:5008"
        readMVar quit

    args <- daemonArgsDefault (fromJust $ parseURI "tcp://localhost:5581")
                               Nothing "test" quit
    linkThread $ startContents args

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
            show (0 :: Address) `shouldBe` "00000000000"
            show (2^64-1 :: Address) `shouldBe` "LygHa16AHYF"
            show (minBound :: Address) `shouldBe` "00000000000"
            show (maxBound :: Address) `shouldBe` "LygHa16AHYF"

        it "decodes an address from base62" $ do
            fromString "00000000000" `shouldBe` (0 :: Address)
            fromString "00000000001" `shouldBe` (1 :: Address)
            fromString "LygHa16AHYF" `shouldBe` ((2^64-1) :: Address)
            fromString "LygHa16AHYG" `shouldBe` (0 :: Address)

    describe "Full stack" $ do
        it "unions two dicts" $ do
            let dict_a = listToDict [("a", "1")]
            let dict_b = listToDict [("a", "2")]
            let addr = 1

            cleanupTestEnvironment

            let o = Origin "PONY"
            xs <- withContentsConnection "localhost" $ \c -> do
                updateSourceDict addr dict_a o c
                updateSourceDict addr dict_b o c
                toListM (enumerateOrigin o c)
            case xs of
                [(addr', dict)] -> do
                    dict `shouldBe` dict_b
                    addr' `shouldBe` addr
                _ -> error "expected one"

        prop "updates source dict for any address" propSourceDictUpdated


listToDict :: [(Text, Text)] -> SourceDict
listToDict elts = either error id . makeSourceDict $ fromList elts

propSourceDictUpdated :: Address -> SourceDict -> Property
propSourceDictUpdated addr dict = monadicIO $ do
    xs <- run $ do
        -- Clear out ceph
        cleanupTestEnvironment
        let o = Origin "PONY"
        withContentsConnection "localhost" $ \c -> do
            updateSourceDict addr dict o c
            toListM (enumerateOrigin o c)
    case xs of
        [(addr', dict')] -> assert (addr' == addr && dict' == dict)
        _                -> error "expected one"
