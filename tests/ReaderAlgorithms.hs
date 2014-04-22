{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Data.List(sort)
import Control.Applicative
import Test.Hspec
import Data.Word
import Test.Hspec.QuickCheck
import Control.Monad.ST
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen
import qualified Vaultaire.ReaderAlgorithms as A
import Vaultaire.ReaderAlgorithms (Point(..))
import Data.Vector.Storable(Vector)
import qualified Data.Vector.Storable as V

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

        it "should retain no duplicates" $ property propNoDuplicates
        it "should sort" $ property propSorted

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
