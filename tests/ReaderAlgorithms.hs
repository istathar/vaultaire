{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Control.Monad.ST
import Data.ByteString (ByteString)
import Data.List (sort)
import Data.Vector.Storable (Vector)
import qualified Data.Vector.Storable as V
import Data.Word
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen
import Vaultaire.ReaderAlgorithms (Point (..))
import qualified Vaultaire.ReaderAlgorithms as A

data AddrStartEnd = AddrStartEnd Word64 Word64 Word64
  deriving Show

instance Arbitrary AddrStartEnd where
    arbitrary = do
        addr  <- elements [0..9]
        start <- div 2 <$> arbitrary `suchThat` (> 0)
        end   <- arbitrary `suchThat` (> start)
        return $ AddrStartEnd addr start end


instance Arbitrary (Vector Point) where
    arbitrary = V.fromList <$> arbitrary

instance Arbitrary Point where
    arbitrary = Point <$> elements [0..9]
                      <*> arbitrary
                      <*> arbitrary
main :: IO ()
main = hspec suite

suite :: Spec
suite = do
    describe "filtering" $ do
        it "has no elements later than end" $ property propFilterNoLater
        it "has no elements earlier than start" $ property propFilterNoEarlier
        it "has all elements it should" $ property propFilterEqual

    describe "deduplication" $ do
        it "must preserve first write" $
           V.thaw (V.fromList [Point 1 2 2, Point 1 2 3, Point 0 0 0])
           >>= A.deDuplicate
           >>= V.freeze
           >>= (`shouldBe` V.fromList [Point 0 0 0, Point 1 2 2])

        it "last must preserve last write" $ 
           V.thaw (V.fromList [Point 1 2 2, Point 1 2 3, Point 0 0 0])
           >>= A.deDuplicateLast
           >>= V.freeze
           >>= (`shouldBe` V.fromList [Point 0 0 0, Point 1 2 3])

        it "should retain no duplicates" $ property propNoDuplicates
        it "should sort" $ property propSorted

    describe "merging" $
        it "correctly merges a pointer record and extended bucket" $
            let merged = runST $ A.mergeSimpleExtended pointerRecord
                                                       extendedRecord
                                                       5
                                                       minBound
                                                       maxBound
            in merged `shouldBe` mergedRecord

pointerRecord :: ByteString
pointerRecord =
    "\x05\x00\x00\x00\x00\x00\x00\x00\
    \\x02\x00\x00\x00\x00\x00\x00\x00\
    \\x02\x00\x00\x00\x00\x00\x00\x00"

extendedRecord :: ByteString
extendedRecord =
    "\x00\x00\
    \\x03\x00\x00\x00\x00\x00\x00\x00\
    \\x41\x42\x43"

mergedRecord :: ByteString
mergedRecord =
    "\x05\x00\x00\x00\x00\x00\x00\x00\
    \\x02\x00\x00\x00\x00\x00\x00\x00\
    \\x03\x00\x00\x00\x00\x00\x00\x00\
    \\&ABC"

propNoDuplicates :: Vector Point -> Bool
propNoDuplicates v =
    noDups $ runST $ V.thaw v >>= A.deDuplicate >>= V.freeze
  where
    noDups v' = V.foldr (\x acc -> acc && V.length (V.filter (== x) v') == 1)
                        True
                        v'

propSorted :: Vector Point -> Bool
propSorted v =
    let s  = V.fromList . sort . V.toList
        v' = runST $ V.thaw v >>= A.deDuplicate >>= V.freeze
    in s v' == v'

propFilterNoLater :: AddrStartEnd -> Vector Point -> Bool
propFilterNoLater (AddrStartEnd addr start end) v =
    let v' = runST $ V.thaw v >>= A.filter addr start end >>= V.freeze
    in V.null $ V.filter ((> end) . A.time) v'

propFilterNoEarlier :: AddrStartEnd -> Vector Point -> Bool
propFilterNoEarlier (AddrStartEnd addr start end) v =
    let v' = runST $ V.thaw v >>= A.filter addr start end >>= V.freeze
    in V.null $ V.filter ((< start) . A.time) v'

propFilterEqual :: AddrStartEnd -> Vector Point -> Bool
propFilterEqual (AddrStartEnd addr start end) v =
    let v'  = runST $ V.thaw v >>= A.filter addr start end >>= V.freeze
        v'' = V.filter filt v

    in v' == v''
  where
    filt p = let t = A.time p in t <= end && t >= start && A.address p == addr
