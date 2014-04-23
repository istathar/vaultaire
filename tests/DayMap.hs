{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString
import Test.Hspec
import Vaultaire.DayMap
import qualified Data.Map as Map

main :: IO ()
main = hspec suite


goodDayFile, badDayFile,nonZeroDayFile :: ByteString
goodDayMap :: DayMap

badDayFile  = "wat?"

nonZeroDayFile  = "AAAAAAAABBBBBBBB"

goodDayFile = "\x00\x00\x00\x00\x00\x00\x00\x00\
              \\x01\x00\x00\x00\x00\x00\x00\x00\&\
              \CCCCCCCCDDDDDDDD"

goodDayMap  = Map.fromList [(0,1)
                           ,(0x4343434343434343, 0x4444444444444444)]


simple :: DayMap
simple = Map.fromList [(0, 100), (10, 200), (30, 300)]

singleEntry :: DayMap
singleEntry = Map.fromList [(0, 100)]

empty :: DayMap
empty = Map.empty

suite :: Spec
suite = do
    describe "loading" $ do
        it "succeeds with good file" $
            loadDayMap goodDayFile `shouldBe` Right goodDayMap

        it "fails with bad file" $
            loadDayMap badDayFile `shouldBe` Left "corrupt contents,\
                \ should be multiple of 16, was: 4 bytes."

        it "fails with empty file" $
            loadDayMap "" `shouldBe` Left "empty"

        it "fails with non zero start" $
            loadDayMap nonZeroDayFile `shouldBe` Left "bad first entry, \
                \must start at zero."

    describe "lookup" $ do
        it "handles left boundary correctly" $ do
            lookupEpoch 0 simple `shouldBe` 0
            lookupNumBuckets 0 simple `shouldBe` 100

        it "handles right boundary correcly" $ do
            lookupEpoch 10 simple `shouldBe` 0
            lookupNumBuckets 10 simple `shouldBe` 100

        it "handles beyond right boundary correcly" $ do
            lookupEpoch 11 simple `shouldBe` 10
            lookupNumBuckets 11 simple `shouldBe` 200

        it "handles beyond end" $ do
            lookupEpoch 31 simple `shouldBe` 30
            lookupNumBuckets 31 simple `shouldBe` 300

        it "handle single entry" $ do
            lookupEpoch 50 singleEntry `shouldBe` 0
            lookupNumBuckets 50 singleEntry `shouldBe` 100
