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

goodDayFile = "AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD"
goodDayMap  = Map.fromList [(0x4141414141414141, 0x4242424242424242)
                           ,(0x4343434343434343, 0x4444444444444444)]

badDayFile  = "wat?"

suite :: Spec
suite =
    describe "loading" $ do
        it "succeeds with good file" $
            loadDayMap goodDayFile `shouldBe` Right goodDayMap

        it "fails with bad file" $
            loadDayMap badDayFile `shouldBe` Left "corrupt"

        it "is empty with empty file" $
            loadDayMap "" `shouldBe` Right Map.empty
