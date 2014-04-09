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
            simpleDayMap `lookupEpoch` 0 `shouldBe` 0
            simpleDayMap `lookupNoBuckets` 0 `shouldBe` 100

        it "handles right boundary correcly" $ do
            simpleDayMap `lookupEpoch` 10 `shouldBe` 0
            simpleDayMap `lookupNoBuckets` 10 `shouldBe` 100

        it "handles beyond right boundary correcly" $ do
            simpleDayMap `lookupEpoch` 11 `shouldBe` 10
            simpleDayMap `lookupNoBuckets` 11 `shouldBe` 200

        it "handles beyond end" $ do
            simpleDayMap `lookupEpoch` 31 `shouldBe` 30
            simpleDayMap `lookupNoBuckets` 31 `shouldBe` 300
