{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString
import qualified Data.Map as Map
import Test.Hspec
import Vaultaire.DayMap

main :: IO ()
main = hspec suite


goodDayFile, badDayFile,nonZeroDayFile :: ByteString
goodDayMap :: DayMap

badDayFile  = "wat?"

nonZeroDayFile  = "AAAAAAAABBBBBBBB"

goodDayFile = "\x00\x00\x00\x00\x00\x00\x00\x00\
              \\x01\x00\x00\x00\x00\x00\x00\x00\&\
              \CCCCCCCCDDDDDDDD"

goodDayMap  = DayMap $ Map.fromList [(0,1)
                           ,(0x4343434343434343, 0x4444444444444444)]


simple :: DayMap
simple = DayMap $ Map.fromList [(0, 100), (10, 200), (30, 300)]

singleEntry :: DayMap
singleEntry = DayMap $ Map.fromList [(0, 100)]

empty :: DayMap
empty = DayMap Map.empty

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
        it "handles left boundary correctly" $
            lookupFirst 0 simple `shouldBe` (0, 100)

        it "handles right boundary correcly" $
            lookupFirst 10 simple `shouldBe` (0, 100)

        it "handles beyond right boundary correcly" $
            lookupFirst 11 simple `shouldBe` (10, 200)

        it "handles beyond end" $
            lookupFirst 31 simple `shouldBe` (30, 300)

        it "handle single entry" $
            lookupFirst 50 singleEntry `shouldBe` (0, 100)

        it "returns ranges" $ do
            lookupRange 0 1 simple `shouldBe` [(0, 100)]
            lookupRange 3 10 simple `shouldBe` [(0, 100)]
            lookupRange 0 11 simple `shouldBe` [(0, 100), (10, 200)]
            lookupRange 3 11 simple `shouldBe` [(0, 100), (10, 200)]
