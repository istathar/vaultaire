{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Control.Monad.Primitive
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.Vector.Algorithms.Merge as M
import Data.Vector.Generic.Mutable (MVector)
import Data.Vector.Storable (Vector)
import qualified Data.Vector.Storable as V
import Data.Vector.Storable.ByteString
import Data.Word
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen
import Vaultaire.ReaderAlgorithms (Point (..))
import qualified Vaultaire.ReaderAlgorithms as A
import Vaultaire.Types (Address (..))

data AddrStartEnd = AddrStartEnd Address Word64 Word64
  deriving Show

instance Arbitrary AddrStartEnd where
    arbitrary = do
        addr  <- Address <$> elements [0..9]
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
    describe "processBucket" $ do
        prop "has no elements later than end" propFilterNoLater
        prop "has no elements earlier than start" propFilterNoEarlier

    describe "first write deduplication" $ do
        it "must preserve first write" $
           V.thaw (V.fromList [Point 0 0 0, Point 1 2 2, Point 1 2 3])
           >>= A.deDuplicate A.similar
           >>= V.freeze
           >>= (`shouldBe` V.fromList [Point 0 0 0, Point 1 2 2])

        prop "should retain no duplicates" $ propNoDuplicates (A.deDuplicate (==))

    describe "last write deduplication" $ do
        it "last must preserve last write" $
           V.thaw (V.fromList [Point 0 0 0, Point 1 2 2, Point 1 2 3])
           >>= A.deDuplicateLast A.similar
           >>= V.freeze
           >>= (`shouldBe` V.fromList [Point 0 0 0, Point 1 2 3])

        prop "should retain no duplicates" $ propNoDuplicates (A.deDuplicateLast (==))

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

type VectorF = (PrimMonad m, MVector v e, Ord e, Eq e) => v (PrimState m) e -> m (v (PrimState m) e)

propNoDuplicates :: VectorF -> Vector Point -> Bool
propNoDuplicates f v =
    noDups $ runST $ V.thaw v >>= (\v' -> M.sort v' >> f v') >>= V.freeze
  where
    noDups v' = V.foldr (\x acc -> acc && V.length (V.filter (== x) v') == 1)
                        True
                        v'

propFilterNoLater :: AddrStartEnd -> Vector Point -> Bool
propFilterNoLater (AddrStartEnd addr start end) v =
    let v' = byteStringToVector $ runST $ A.processBucket (vectorToByteString v)addr start end
    in V.null $ V.filter ((> end) . A.time) v'

propFilterNoEarlier :: AddrStartEnd -> Vector Point -> Bool
propFilterNoEarlier (AddrStartEnd addr start end) v =
    let v' = byteStringToVector $ runST $ A.processBucket (vectorToByteString v) addr start end
    in V.null $ V.filter ((< start) . A.time) v'
