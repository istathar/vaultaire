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

contentsOperationIdentity :: ContentsOperation -> Bool
contentsOperationIdentity op = id' op == op
  where
    id' = fromRight . fromWire . toWire
    fromRight = either (error . show) id
