{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.List (foldl')
import Data.Monoid
import Data.Word
import Test.QuickCheck
import TestHelpers

data TestPoint = SimplePoint
    { address :: Word64
    , time    :: Word64
    , payload :: Word64
    }
    | ExtendedPoint
    { address :: Word64
    , time    :: Word64
    , len     :: Word64
    , bytes   :: ByteString
    }
  deriving (Show, Eq, Ord)

newtype UniqueTestPoints = UniqueTestPoints [TestPoint]
  deriving (Show)

instance Arbitrary TestPoint where
    arbitrary = do
        extended <- arbitrary
        if extended
            then do
                bytes <- arbitrary
                ExtendedPoint <$> arbitrary `suchThat` odd
                              <*> arbitrary
                              <*> pure (fromIntegral $ BS.length bytes)
                              <*> pure bytes
            else SimplePoint <$> arbitrary `suchThat` even
                             <*> arbitrary
                             <*> arbitrary

nubTest :: TestPoint -> TestPoint -> Bool
nubTest SimplePoint{} ExtendedPoint{} = False
nubTest ExtendedPoint{} SimplePoint{} = False
nubTest (SimplePoint addr1 time1 _) (SimplePoint addr2 time2 _) =
    addr1 == addr2 && time1 == time2
nubTest (ExtendedPoint addr1 time1 _ _) (ExtendedPoint addr2 time2 _ _) =
    addr1 == addr2 && time1 == time2

addrTest :: TestPoint -> TestPoint -> Bool
addrTest SimplePoint{} ExtendedPoint{} = False
addrTest ExtendedPoint{} SimplePoint{} = False
addrTest (SimplePoint addr1 _ _) (SimplePoint addr2 _ _) =
    addr1 == addr2
addrTest (ExtendedPoint addr1 _ _ _) (ExtendedPoint addr2 _ _ _) =
    addr1 == addr2

toPayload :: [TestPoint] -> ByteString
toPayload = toStrict . toLazyByteString . foldl' build mempty
  where
    build acc SimplePoint{..} = acc <> word64LE address <> word64LE time <> word64LE payload
    build acc ExtendedPoint{..} = acc <> word64LE address <> word64LE time <> word64LE len <> byteString bytes

instance Arbitrary ByteString where
    arbitrary = BS.pack <$> arbitrary

main :: IO ()
main = do
    runTestDaemon "tcp://localhost:1234" (return ())
    startTestDaemons
    -- result <- quickCheckResult propWriteThenRead
    -- unless (isSuccess result) exitFailure

