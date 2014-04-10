{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString
import Test.Hspec
import Vaultaire.DayMap
import qualified Data.Map as Map

main :: IO ()
main = hspec suite


goodDayFile, badDayFile :: ByteString
goodDayMap :: DayMap

badDayFile  = "wat?"
goodDayFile = "AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD"
goodDayMap  = Map.fromList [(0x4141414141414141, 0x4242424242424242)
                           ,(0x4343434343434343, 0x4444444444444444)]


simpleDayMap :: DayMap
simpleDayMap = Map.fromList [(0, 100), (10, 200), (30, 300)]

suite :: Spec
suite = do
    describe "loading" $ do
        it "succeeds with good file" $
            loadDayMap goodDayFile `shouldBe` Right goodDayMap

        it "fails with bad file" $
            loadDayMap badDayFile `shouldBe` Left "corrupt"

        it "is empty with empty file" $
            loadDayMap "" `shouldBe` Right Map.empty

    describe "lookup" $ do
        it "handles left boundary correctly" $ do
            lookupEpoch 0 simpleDayMap `shouldBe` 0
            lookupNoBuckets 0 simpleDayMap `shouldBe` 100

        it "handles right boundary correcly" $ do
            lookupEpoch 10 simpleDayMap `shouldBe` 0
            lookupNoBuckets 10 simpleDayMap `shouldBe` 100

        it "handles beyond right boundary correcly" $ do
            lookupEpoch 11 simpleDayMap `shouldBe` 10
            lookupNoBuckets 11 simpleDayMap `shouldBe` 200

        it "handles beyond end" $ do
            lookupEpoch 31 simpleDayMap `shouldBe` 30
            lookupNoBuckets 31 simpleDayMap `shouldBe` 300
