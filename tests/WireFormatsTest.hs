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

{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative ((<$>), (<*>))
import Control.Exception (throw)
import Data.HashMap.Strict (fromList)
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen
import Test.QuickCheck.Instances ()
import Vaultaire.Types

instance Arbitrary Address where
    arbitrary = Address <$> arbitrary

instance Arbitrary SourceDict where
    arbitrary = do
        attempt <- arbitrary
        either (const arbitrary) return $ makeSourceDict attempt

instance Arbitrary ContentsOperation where
    arbitrary = oneof [ return ContentsListRequest
                      , return GenerateNewAddress
                      , UpdateSourceTag <$> arbitrary <*> arbitrary
                      , RemoveSourceTag <$> arbitrary <*> arbitrary ]

main :: IO ()
main = hspec suite

suite :: Spec
suite = do
    describe "contents wire format" $
        it "toWire . fromWire == id" $ property contentsOperationIdentity

    describe "source dict wire format" $ do
        let hm =  fromList [ ("metric","cpu")
                           , ("server","example")]
        let expected = either error id $ makeSourceDict hm
        it "parses string to map" $
            let wire = either throw id $ fromWire "server:example,metric:cpu"
            in wire `shouldBe` expected

    describe "Contents list reply" $
      let
        (Right source_dict) = makeSourceDict $ fromList [("metric","cpu"), ("server","www.example.com")]
        encoded = toWire source_dict
        expected    = "\x02\
                       \\x01\x00\x00\x00\x00\x00\x00\x00\
                       \\x22\x00\x00\x00\x00\x00\x00\x00\
                       \metric:cpu,server:www.example.com,"
      in
        it "toWire for ContentsListEntry is the same as ContentsListBypass" $ do
            toWire (ContentsListEntry 1 source_dict) `shouldBe` expected
            toWire (ContentsListBypass 1 encoded) `shouldBe` expected


contentsOperationIdentity :: ContentsOperation -> Bool
contentsOperationIdentity op = id' op == op
  where
    id' = fromRight . fromWire . toWire
    fromRight = either (error . show) id

