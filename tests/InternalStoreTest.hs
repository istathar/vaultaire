{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.Function (on)
import Data.List (foldl', groupBy, nubBy, sort)
import Data.List
import Control.Monad.State.Strict
import Pipes.Parse
import Data.List.NonEmpty (fromList)
import Data.Monoid
import Data.Word
import System.Exit
import System.ZMQ4.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import Test.QuickCheck.Test
import TestHelpers (runTestDaemon)
import Vaultaire.CoreTypes
import Test.Hspec
import Test.Hspec.QuickCheck
import Vaultaire.Daemon
import Data.Locator
import Vaultaire.InternalStore (enumerateOrigin, readFrom, writeTo)
import TestHelpers
import Vaultaire.OriginMap
import System.Rados.Monadic(objects)

instance Arbitrary ByteString where
    arbitrary = BS.pack <$> arbitrary

instance Arbitrary Origin where
    -- suchThat condition should be removed once locators package is fixed
    arbitrary = Origin . BS.pack . toLocator16a 6 <$> arbitrary `suchThat` (>0)

instance Arbitrary Address where
    arbitrary = Address <$> arbitrary

main :: IO ()
main = hspec suite

suite :: Spec
suite = do
    describe "writing" $ do
        it "writes simple bucket correctly" $ do
            runTestDaemon "tcp://localhost:1234" $ writeTo (Origin "PONY") (Address 4) "Hai"
            readObject "02_PONY_INTERNAL_00000000000000000004_00000000000000000000_simple"
            >>= (`shouldBe` Right "\x04\x00\x00\x00\x00\x00\x00\x00\
                                  \\x00\x00\x00\x00\x00\x00\x00\x00\
                                  \\x00\x00\x00\x00\x00\x00\x00\x00")

        it "writes extended bucket correctly" $ do
            runTestDaemon "tcp://localhost:1234" $ writeTo (Origin "PONY") (Address 4) "Hai"
            readObject "02_PONY_INTERNAL_00000000000000000004_00000000000000000000_extended"
            >>= (`shouldBe` Right "\x03\x00\x00\x00\x00\x00\x00\x00\&Hai")

    describe "reading" $  do
        it "reads a write" $ do -- Use the same write, as we have already shown it correct
            runTestDaemon "tcp://localhost:1234" $ do
                writeTo (Origin "PONY") (Address 4) "Hai"
                readFrom (Origin "PONY") (Address 4)
            >>= (`shouldBe` Just "Hai")

    describe "enumeration" $ do
        it "enumerates two writes" $ do
            addrs <- runTestDaemon "tcp://localhost:1234" $ do
                writeTo (Origin "PONY") (Address 128) "Hai1"
                writeTo (Origin "PONY") (Address 0) "Hai2"
                writeTo (Origin "PONY") (Address 128) "Hai3" -- overwrite
                liftPool objects >>= liftIO . print

                evalStateT drawAll (enumerateOrigin "PONY")
            addrs `shouldBe` [((Address 0), "Hai2"), ((Address 128), "Hai3")]

    describe "identity QuickCheck" $
        it "writes then reads" $ property propWriteThenRead

propWriteThenRead :: (Origin, Address, ByteString) -> Property
propWriteThenRead arb@(_,_,payload) = monadicIO $ do
    payload' <- run $ runTestDaemon "tcp://localhost:1234" $ writeThenRead arb
    assert $ payload == payload'

writeThenRead :: (Origin, Address, ByteString) -> Daemon ByteString
writeThenRead (o,a,p) = do
        writeTo o a p
        readFrom o a >>= maybe (error "no value") return
