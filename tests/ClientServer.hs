{-# LANGUAGE OverloadedStrings     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Marquise.IO
import Marquise.Client
import Test.Hspec

ns1, ns2, ns3 :: NameSpace
ns1 = either error id $ mkNameSpace "ns1"
ns2 = either error id $ mkNameSpace "ns2"
ns3 = either error id $ mkNameSpace "ns3"

main :: IO ()
main = hspec $ do
    describe "IO MarquiseMonad" $ do
        it "returns nothing on empty file" $
            nextBurst ns3 >>= (`shouldBe` Nothing)

        it "reads two appends, then cleans up when nextBurst is called" $ do
            append ns1 "BBBBBBBBAAAAAAAACCCCCCCC"
            append ns1 "DBBBBBBBAAAAAAAACCCCCCCC"
            append ns2 "FBBBBBBBAAAAAAAACCCCCCCC"

            Just (bp1,bytes1) <- nextBurst ns1
            Just (bp2,bytes2) <- nextBurst ns2

            bytes1 `shouldBe` "BBBBBBBBAAAAAAAACCCCCCCC\
                              \DBBBBBBBAAAAAAAACCCCCCCC"
            bytes2 `shouldBe` "FBBBBBBBAAAAAAAACCCCCCCC"

            flagSent bp1
            flagSent bp2

            nextBurst ns1 >>= (`shouldBe` Nothing)
            nextBurst ns2 >>= (`shouldBe` Nothing)
