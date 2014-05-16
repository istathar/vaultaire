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
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where

import qualified Data.ByteString as B
import qualified Data.HashMap.Strict as HashMap
import System.ZMQ4.Monadic hiding (Event)

import Test.Hspec hiding (pending)

import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.CoreTypes
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
    describe "Requests" $ do
        it "Operations encode correctly" $ do
            opcodeToWord64 ContentsListRequest `shouldBe`           0x0
            opcodeToWord64 RegisterNewAddress `shouldBe`                0x1
            opcodeToWord64 (UpdateSourceTag 0 HashMap.empty) `shouldBe`   0x2
            opcodeToWord64 (RemoveSourceTag 0 HashMap.empty) `shouldBe`   0x3

    describe "Source dictionaries" $ do
        let s = HashMap.fromList [("metric","cpu"), ("server","www.example.com")]

        it "parses string to map" $ do
            handleSourceArgument "server:www.example.com,metric:cpu" `shouldBe` s

        it "encodes map to string" $ do
            encodeSourceDict s `shouldBe` "metric:cpu,server:www.example.com"

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

    describe "Contents list reply" $
      let
        a1  = 1
        s1  = HashMap.fromList [("metric","cpu"), ("server","www.example.com")]
        s1' = encodeSourceDict s1
        a2  = 2
        s2  = HashMap.fromList [("metric","eth0"), ("server","db3.example.com")]
        s2' = encodeSourceDict s2
      in do
        it "single source response correctly encoded" $ do
            encodeReply (a1,s1') `shouldBe`
                "\x01\x00\x00\x00\x00\x00\x00\x00\
                \\x21\x00\x00\x00\x00\x00\x00\x00\
                \metric:cpu,server:www.example.com"

        it "multiple sources response correctly encoded" $ do
            B.concat [encodeReply (a1,s1'), encodeReply (a2,s2')] `shouldBe`
                "\x01\x00\x00\x00\x00\x00\x00\x00\
                \\x21\x00\x00\x00\x00\x00\x00\x00\
                \metric:cpu,server:www.example.com\
                \\x02\x00\x00\x00\x00\x00\x00\x00\
                \\x22\x00\x00\x00\x00\x00\x00\x00\
                \metric:eth0,server:db3.example.com"

