{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Applicative
import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import qualified Data.HashMap.Strict as HashMap
import Data.List (sort)
import Data.Monoid
import Data.Time
import Pipes
import Pipes.Lift
import Pipes.Parse
import System.Rados.Monadic
import System.ZMQ4.Monadic hiding (Event)
import Test.Hspec hiding (pending)
import TestHelpers
import Vaultaire.Broker
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.Util
import Vaultaire.Writer

main :: IO ()
main = do
    now <- getCurrentTime
    hspec (suite now)

suite :: UTCTime -> Spec
suite now = do
    describe "appendSimple" $ do
        it "generates a single builder from one append" $ do
            let (_, st) = go $ appendSimple 0 1 "HAI"
            let builder = HashMap.lookup 0 (simple st) >>= HashMap.lookup 1
            case builder of Nothing -> error "lookup"
                            Just b  -> toLazyByteString b `shouldBe` "HAI"

        it "generates a single builder from one append" $ do
            let (_, st) = go $ appendSimple 0 1 "A" >> appendSimple 0 1 "B"
            let builder = HashMap.lookup 0 (simple st) >>= HashMap.lookup 1
            case builder of Nothing -> error "lookup"
                            Just b  -> toLazyByteString b `shouldBe` "AB"

    describe "appendExtended" $
        it "creates appropriate builders and extended map" $ do
            let (_, st) = go $ appendExtended 0 1 0x42 0x90 2 "BC"

            let ext_bytes = "\x02\x00\x00\x00\x00\x00\x00\x00\&BC"
            let ext = HashMap.lookup 0 (extended st) >>= HashMap.lookup 1
            case ext of Nothing -> error "lookup extended"
                        Just b  -> toLazyByteString b `shouldBe` ext_bytes

            let pend = HashMap.lookup 0 (pending st) >>= HashMap.lookup 1
            let pend_bytes = "\x42\x00\x00\x00\x00\x00\x00\x00\
                             \\x90\x00\x00\x00\x00\x00\x00\x00\
                             \\10\x00\x00\x00\x00\x00\x00\x00"
            case pend of
                Nothing -> error "lookup pending"
                Just fs -> let b = mconcat $ map ($10) (snd fs)
                           in toLazyByteString b `shouldBe` pend_bytes

    describe "processPoints" $ do
        it "handles multiple simple points" $ do
            let (latest, st) = go $ processPoints 0 simpleCompound
                                                    startDayMaps "PONY" 0 0
            HashMap.null (extended st) `shouldBe` True
            HashMap.null (pending st) `shouldBe` True
            HashMap.null (simple st) `shouldBe` False
            let norm = HashMap.lookup 0 (simple st) >>= HashMap.lookup 4
            case norm of Nothing -> error "bucket got lost"
                         Just b -> toStrict (toLazyByteString b)

                                   `shouldBe` simpleCompound

            HashMap.null (simple st) `shouldBe` False
            latest `shouldBe` (2, 0)

        it "handles multiple simple and extended points" $ do
            let (latest, st) = go $ processPoints 0 extendedCompound
                                                      startDayMaps "PONY" 0 0
            HashMap.null (extended st) `shouldBe` False
            HashMap.null (pending st) `shouldBe` False
            HashMap.null (simple st) `shouldBe` False

            -- Simple bucket should have only simple points
            let norm = HashMap.lookup 0 (simple st) >>= HashMap.lookup 4
            case norm of Nothing -> error "simple bucket got lost"
                         Just b -> toStrict (toLazyByteString b)
                                   `shouldBe` simpleMessage

            -- Extended bucket should have the length and string
            let ext = HashMap.lookup 0 (extended st) >>= HashMap.lookup 4
            case ext of
                Nothing -> error "extended bucket got lost"
                Just b -> toStrict (toLazyByteString b)
                          `shouldBe` extendedBytes

            -- Pending bucket should have a closure that creates a pointer to
            -- the extended bucket given an offset. These should point to 0x0
            -- and then 0x27 (length 0x1f + header 0x8) reference the offset.
            -- (so 0x2 0x21 given os 0x2)
            --
            -- Note that to achieve the expected ordering we must reverse the
            -- list before concatenation. This is due to prepending to the list
            -- for efficiency.

            let pend = HashMap.lookup 0 (pending st) >>= HashMap.lookup 4
            case pend of
                Nothing -> error "lookup pending"
                Just fs -> let b = mconcat . reverse $ map ($2) (snd fs)
                           in toStrict (toLazyByteString b) `shouldBe` pendingBytes

            latest `shouldBe` (2,3)

    describe "processMessage" $ do
        it "yields state immediately with expired time" $ do
            writes <- evalStateT drawAll $
                yieldEvents >-> evalStateP (startState now) (processEvents 0)

            length writes `shouldBe` 1
            let w = head writes
            HashMap.null (extended w) `shouldBe` False
            HashMap.null (pending w) `shouldBe` False
            HashMap.null (simple w) `shouldBe` False

        it "does not yield state immmediately with a higher batch period" $ do
            writes <- evalStateT drawAll $
                yieldEvents >-> evalStateP (startState now) (processEvents 1)

            null writes `shouldBe` True

    describe "full stack" $
        it "writes a message to disk immediately" $ do
            -- Clean up latest files so that we can test that we are writing
            -- correct values
            _ <- runTestDaemon "tcp://localhost:1234" $ liftPool $ do
                _ <- runObject "02_PONY_extended_latest" remove
                runObject "02_PONY_simple_latest" remove

            linkThread $ runZMQ $ startProxy
                (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
            linkThread $ startWriter "tcp://localhost:5561" Nothing "test" 0
            sendTestMsg >>= (`shouldBe` ["\x42", ""])

            let expected = sort [ "02_PONY_00000000000000000004_00000000000000000000_extended"
                                , "02_PONY_00000000000000000004_00000000000000000000_simple"
                                , "02_PONY_extended_latest"
                                , "02_PONY_simple_latest"
                                , "02_PONY_simple_days"
                                , "02_PONY_write_lock"
                                , "02_PONY_extended_days"]

            runTestPool (sort <$> objects) >>= (`shouldBe` expected)

            sim <- runTestPool $
                runObject "02_PONY_00000000000000000004_00000000000000000000_simple" readFull
            sim `shouldBe` Right (extendedPointers `BS.append` simpleMessage)

            ext <- runTestPool $
                runObject "02_PONY_00000000000000000004_00000000000000000000_extended" readFull
            ext `shouldBe` Right extendedBytes

            runTestPool (runObject "02_PONY_extended_latest" readFull)
                >>= (`shouldBe` Right "\x03\x00\x00\x00\x00\x00\x00\x00")

            runTestPool (runObject "02_PONY_simple_latest" readFull)
                >>= (`shouldBe` Right "\x02\x00\x00\x00\x00\x00\x00\x00")

  where
    go = flip runState (startState now)

extendedBytes :: ByteString
extendedBytes = "\x1f\x00\x00\x00\x00\x00\x00\x00\
                \\&This computer is made of warms.\
                \\x04\x00\x00\x00\x00\x00\x00\x00\
                \\&Yay!"

extendedPointers :: ByteString
extendedPointers = "\x05\x00\x00\x00\x00\x00\x00\x00\
                   \\x02\x00\x00\x00\x00\x00\x00\x00\
                   \\x00\x00\x00\x00\x00\x00\x00\x00\
                   \\x05\x00\x00\x00\x00\x00\x00\x00\
                   \\x03\x00\x00\x00\x00\x00\x00\x00\
                   \\x27\x00\x00\x00\x00\x00\x00\x00"

yieldEvents :: Monad m => Producer Event m ()
yieldEvents = do
    yield (Msg $ Message undefined "PONY" extendedCompound)
    yield Tick

pendingBytes :: ByteString
pendingBytes = "\x05\x00\x00\x00\x00\x00\x00\x00\
               \\x02\x00\x00\x00\x00\x00\x00\x00\
               \\x02\x00\x00\x00\x00\x00\x00\x00\
               \\x05\x00\x00\x00\x00\x00\x00\x00\
               \\x03\x00\x00\x00\x00\x00\x00\x00\
               \\x29\x00\x00\x00\x00\x00\x00\x00"

startDayMaps :: (DayMap, DayMap)
startDayMaps =
    let norm = loadDayMap "\x00\x00\x00\x00\x00\x00\x00\x00\
                           \\x42\x00\x00\x00\x00\x00\x00\x00"
        ext  = loadDayMap "\x00\x00\x00\x00\x00\x00\x00\x00\
                           \\x42\x00\x00\x00\x00\x00\x00\x00"
    in either error id $ (,) <$> norm <*> ext

startState :: UTCTime -> BatchState
startState = BatchState mempty mempty mempty mempty 0 0 startDayMaps
