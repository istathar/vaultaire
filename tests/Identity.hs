{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.List (foldl', groupBy, nubBy, sort)
import Data.List.NonEmpty (fromList)
import Data.Monoid
import Data.Word
import System.Exit
import System.ZMQ4.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import Test.QuickCheck.Test
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
    result <- quickCheckResult propWriteThenRead
    unless (isSuccess result) exitFailure

propWriteThenRead :: [TestPoint] -> Property
propWriteThenRead ps = monadicIO $ do
    run $ do
        runTestDaemon "tcp://localhost:1234" (return ())
        sendPoints ps

    let unique_groups = map sort $ groupBy addrTest $ sort $ nubBy nubTest ps

    forM (filter (not . null) unique_groups) $ \group -> do
        stored <- run $ getPoints (address $ head group) minBound maxBound
        assert $ stored == toPayload group

encodeRequest :: Word64 -> Word64 -> Word64 -> ByteString
encodeRequest addr start end = toStrict $ toLazyByteString $
    word64LE 0 <> word64LE addr <> word64LE start <> word64LE end

getPoints :: Word64 -> Word64 -> Word64 -> IO ByteString
getPoints addr start end = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5570"
    sendMulti s $ fromList ["\x43", "PONY", encodeRequest addr start end]
    ["\x43", msg] <- receiveMulti s
    if BS.null msg
        then return ""
        else do
            ["\x43", ""] <- receiveMulti s
            if BS.take 8 msg /= "\x02\x00\x00\x00\x00\x00\x00\x00"
                then error "Non-response header"
                else return $ BS.drop 8 msg

sendPoints :: [TestPoint] -> IO ()
sendPoints [] = return ()
sendPoints ps = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    sendMulti s $ fromList ["\x42", "PONY", toPayload ps]
    void $ receiveMulti s
