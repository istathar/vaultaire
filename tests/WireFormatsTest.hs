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

{-# LANGUAGE OverloadedStrings     #-}
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

instance Arbitrary ContentsResponse where
    arbitrary = oneof [ RandomAddress  <$> arbitrary
                      , return InvalidContentsOrigin
                      , ContentsListEntry <$> arbitrary <*> arbitrary
                      , return EndOfContentsList
                      , return UpdateSuccess
                      , return RemoveSuccess ]

instance Arbitrary WriteResult where
    arbitrary = oneof [ return InvalidWriteOrigin, return OnDisk ]

instance Arbitrary ReadStream where
    arbitrary = oneof [ return InvalidReadOrigin
                      , SimpleBurst <$> arbitrary
                      , ExtendedBurst <$> arbitrary
                      , return EndOfStream ]

main :: IO ()
main = hspec suite

suite :: Spec
suite = do
    describe "WireFormat identity tests" $ do
        it "ContentsOperation" $ property (wireId :: ContentsOperation -> Bool)
        it "ContentsResponse" $ property (wireId :: ContentsResponse -> Bool)
        it "WriteResult" $ property (wireId :: WriteResult -> Bool)
        it "ReadStream" $ property (wireId :: ReadStream -> Bool)
        it "Address" $ property (wireId :: Address -> Bool)
        it "SourceDict" $ property (wireId :: SourceDict -> Bool)

    describe "source dict wire format" $ do
        let hm =  fromList [ ("metric","cpu")
                           , ("server","example")]
        let expected = either error id $ makeSourceDict hm
        it "parses string to map" $
            let wire = either throw id $ fromWire "server:example,metric:cpu"
            in wire `shouldBe` expected

    describe "Contents list reply" $ do
        it "toWire for ContentsListEntry is the same as ContentsListBypass" $ do
            let al = [("metric","cpu"), ("server","www.example.com")]
            let (Right source_dict) = makeSourceDict $ fromList al
            let encoded = toWire source_dict
            let expected = "\x02\
                           \\x01\x00\x00\x00\x00\x00\x00\x00\
                           \\x22\x00\x00\x00\x00\x00\x00\x00\
                           \metric:cpu,server:www.example.com,"

            toWire (ContentsListEntry 1 source_dict) `shouldBe` expected
            toWire (ContentsListBypass 1 encoded) `shouldBe` expected


wireId :: (Eq w, WireFormat w) => w -> Bool
wireId op = id' op == op
  where
    id' = fromRight . fromWire . toWire
    fromRight = either (error . show) id

