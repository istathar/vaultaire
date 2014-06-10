{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import ArbitraryInstances ()
import Control.Monad
import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import Pipes.Parse
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import TestHelpers
import Vaultaire.Daemon
import Vaultaire.InternalStore (enumerateOrigin, internalStoreBuckets,
                                readFrom, writeTo)
import Vaultaire.Types

rollOverAddress :: Address
rollOverAddress = Address internalStoreBuckets

main :: IO ()
main = hspec suite

suite :: Spec
suite = do
    describe "writing" $ do
        it "writes simple bucket correctly" $ do
            runTestDaemon "tcp://localhost:1234" $ writeTo (Origin "PONY") 4 "Hai"
            readObject "02_PONY_INTERNAL_00000000000000000004_00000000000000000000_simple"
            >>= (`shouldBe` Right "\x04\x00\x00\x00\x00\x00\x00\x00\
                                  \\x00\x00\x00\x00\x00\x00\x00\x00\
                                  \\x00\x00\x00\x00\x00\x00\x00\x00")

        it "writes extended bucket correctly" $ do
            runTestDaemon "tcp://localhost:1234" $ writeTo (Origin "PONY") 4 "Hai"
            readObject "02_PONY_INTERNAL_00000000000000000004_00000000000000000000_extended"
            >>= (`shouldBe` Right "\x03\x00\x00\x00\x00\x00\x00\x00\&Hai")

    describe "reading" $ do
        it "reads a write" $ -- Use the same write, as we have already shown it correct
            runTestDaemon "tcp://localhost:1234"
                (do writeTo (Origin "PONY") 4 "Hai"
                    readFrom (Origin "PONY") 4)
            >>= (`shouldBe` Just "Hai")

        it "disambiguates collision" $
            runTestDaemon "tcp://localhost:1234"
                (do writeTo (Origin "PONY") rollOverAddress "Hai1"
                    readFrom (Origin "PONY") 0)
            >>= (`shouldBe` Nothing)

    describe "enumeration" $
        it "enumerates two writes" $ do
            addrs <- runTestDaemon "tcp://localhost:1234" $ do
                writeTo (Origin "PONY") rollOverAddress "Hai1"
                writeTo (Origin "PONY") 0 "Hai2"
                writeTo (Origin "PONY") rollOverAddress "Hai3" -- overwrite

                evalStateT drawAll (enumerateOrigin "PONY")
            addrs `shouldBe` [(0, "Hai2"), (rollOverAddress, "Hai3")]

    describe "identity QuickCheck" $
        it "writes then reads" $ property propWriteThenRead

propWriteThenRead :: (Origin, Address, ByteString) -> Property
propWriteThenRead arb@(_,_,payload) = monadicIO $ do
    (enumeration, read') <- run $ runTestDaemon "tcp://localhost:1234" $ writeThenRead arb
    assert $ (enumeration == read') && (read' == payload)

writeThenRead :: (Origin, Address, ByteString) -> Daemon (ByteString, ByteString)
writeThenRead (o,a,p) = do
        writeTo o a p
        [(a', e)] <- evalStateT drawAll (enumerateOrigin o)
        unless (a' == a) $ error "invalid address from enumeration"
        r <- readFrom o a >>= maybe (error "no value") return
        return (e,r)
