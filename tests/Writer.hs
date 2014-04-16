{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString.Lazy.Builder
import Data.List.NonEmpty (fromList)
import Data.ByteString.Lazy(toStrict)
import Data.Monoid
import Test.Hspec hiding (pending)
import Control.Monad.State.Strict
import qualified Data.HashMap.Strict as HashMap
import System.Rados.Monadic
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Control.Applicative
import Pipes
import Vaultaire.Broker
import Pipes.Lift
import Pipes.Parse
import Vaultaire.Writer
import Vaultaire.Daemon
import System.ZMQ4.Monadic hiding (Event)
import Vaultaire.Util
import Vaultaire.DayMap
import Data.Time
import TestHelpers

main :: IO ()
main = do
    now <- getCurrentTime
    hspec (suite now)

suite :: UTCTime -> Spec
suite now = do
    describe "appendSimple" $ do
        it "generates a single builder from one append" $ do
            let st = go $ appendSimple 0 1 "HAI"
            let builder = HashMap.lookup 0 (normal st) >>= HashMap.lookup 1
            case builder of Nothing -> error "lookup"
                            Just b  -> toLazyByteString b `shouldBe` "HAI"

        it "generates a single builder from one append" $ do
            let st = go $ appendSimple 0 1 "A" >> appendSimple 0 1 "B"
            let builder = HashMap.lookup 0 (normal st) >>= HashMap.lookup 1
            case builder of Nothing -> error "lookup"
                            Just b  -> toLazyByteString b `shouldBe` "AB"

    describe "appendExtended" $ do
        it "creates appropriate builders and extended map" $ do
            let st = go $ appendExtended 0 1 0x42 0x90 2 "BC"

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
        it "handles multiple normal points" $ do
            let st = go $ processPoints 0 normalCompound startDayMaps "PONY"
            (HashMap.null $ extended st) `shouldBe` True
            (HashMap.null $ pending st) `shouldBe` True
            (HashMap.null $ normal st) `shouldBe` False
            let norm = HashMap.lookup 0 (normal st) >>= HashMap.lookup 4
            case norm of Nothing -> error "bucket got lost"
                         Just b -> (toStrict $ toLazyByteString b)
                                   `shouldBe` normalCompound
            (HashMap.null $ normal st) `shouldBe` False

        it "handles multiple normal and extended points" $ do
            let st = go $ processPoints 0 extendedCompound startDayMaps "PONY"
            (HashMap.null $ extended st) `shouldBe` False
            (HashMap.null $ pending st) `shouldBe` False
            (HashMap.null $ normal st) `shouldBe` False

            -- Simple bucket should have only simple points
            let norm = HashMap.lookup 0 (normal st) >>= HashMap.lookup 4
            case norm of Nothing -> error "simple bucket got lost"
                         Just b -> (toStrict $ toLazyByteString b)
                                   `shouldBe` normalCompound

            -- Extended bucket should have the length and string
            let ext = HashMap.lookup 0 (extended st) >>= HashMap.lookup 4
            case ext of
                Nothing -> error "extended bucket got lost"
                Just b -> (toStrict $ toLazyByteString b)
                          `shouldBe` "\x1f\x00\x00\x00\x00\x00\x00\x00\
                                      \\&This computer is made of warms.\
                                      \\x04\x00\x00\x00\x00\x00\x00\x00\
                                      \\&Yay!"

            -- Pending bucket should have a closure that creates a pointer to
            -- the extended bucket given an offset. These should point to 0x0
            -- and then 0x1f reference the offset. (so 0x2 0x21 given os 0x2)
            --
            -- Note that to achieve the expected ordering we must reverse the
            -- list before concatenation. This is due to prepending to the list
            -- for efficiency.

            let pend = HashMap.lookup 0 (pending st) >>= HashMap.lookup 4
            case pend of
                Nothing -> error "lookup pending"
                Just fs -> let b = mconcat . reverse $ map ($2) (snd fs)
                           in (toStrict $ toLazyByteString b) `shouldBe` pendingBytes

    describe "processMessage" $ do
        it "yields state immediately with expired time" $ do
            writes <- evalStateT drawAll $
                yieldEvents >-> evalStateP (startState now) (processEvents 0)

            length writes `shouldBe` 1
            let w = head writes
            (HashMap.null $ extended w) `shouldBe` False
            (HashMap.null $ pending w) `shouldBe` False
            (HashMap.null $ normal w) `shouldBe` False

        it "does not yield state immmediately with a higher batch period" $ do
            writes <- evalStateT drawAll $
                yieldEvents >-> evalStateP (startState now) (processEvents 1)

            null writes `shouldBe` True

    describe "full stack" $ do
        it "writes a message to disk immediately" $ do
            linkThread $ runZMQ $ startProxy
                (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
            linkThread $ startWriter "tcp://localhost:5561" Nothing "test" 0
            sendTestMsg >>= (`shouldBe` ["\x42", ""])
            bucket <- runTestDaemon "tcp://localhost:1234" $ liftPool $
                runObject "02_PONY_00000000000000000004_00000000000000000000_simple" readFull
            bucket `shouldBe` Right (normalMessage `BS.append` pendingBytes)
  where
    go = (flip execState) (startState now)

sendTestMsg :: IO [ByteString]
sendTestMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    -- Simulate a client sending a sequence number and message
    sendMulti s $ fromList ["\x42", "PONY", extendedCompound]
    receiveMulti s

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
               \\x21\x00\x00\x00\x00\x00\x00\x00"

extendedCompound, normalCompound, normalMessage, extendedMessage :: ByteString

extendedCompound = normalCompound `BS.append` extendedMessage

normalCompound = normalMessage `BS.append` normalMessage

normalMessage =
    "\x04\x00\x00\x00\x00\x00\x00\x00\
    \\x02\x00\x00\x00\x00\x00\x00\x00\
    \\x01\x00\x00\x00\x00\x00\x00\x00"

extendedMessage =
    "\x05\x00\x00\x00\x00\x00\x00\x00\
    \\x02\x00\x00\x00\x00\x00\x00\x00\
    \\x1f\x00\x00\x00\x00\x00\x00\x00\
    \\&This computer is made of warms.\
    \\x05\x00\x00\x00\x00\x00\x00\x00\
    \\x03\x00\x00\x00\x00\x00\x00\x00\
    \\x04\x00\x00\x00\x00\x00\x00\x00\
    \\&Yay!"

startDayMaps :: (DayMap, DayMap)
startDayMaps =
    let norm = loadDayMap "\x00\x00\x00\x00\x00\x00\x00\x00\
                           \\x42\x00\x00\x00\x00\x00\x00\x00"
        ext  = loadDayMap "\x00\x00\x00\x00\x00\x00\x00\x00\
                           \\x42\x00\x00\x00\x00\x00\x00\x00"
    in either error id $ (,) <$> norm <*> ext

startState :: UTCTime -> BatchState
startState = BatchState mempty mempty mempty mempty startDayMaps
