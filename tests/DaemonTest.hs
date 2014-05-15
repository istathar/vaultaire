{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Data.ByteString (ByteString)
import Data.List.NonEmpty (fromList)
import System.Rados.Monadic hiding (async)
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec
import TestHelpers
import Vaultaire.Broker
import Vaultaire.Daemon hiding (async)
import Vaultaire.DayMap
import Vaultaire.CoreTypes (Origin (..))
import Vaultaire.RollOver
import Vaultaire.Util

main :: IO ()
main = do
    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"

    hspec suite

-- | A pre-requisite for this test suite is a connection to a test ceph cluster
-- with a "test" pool.
suite :: Spec
suite = do
    describe "Daemon messaging" $ do
        it "starts up and shuts down cleanly" $
            runTestDaemon "tcp://localhost:1234" (return ())
            >>= (`shouldBe` ())

        it "ignores bad message and replies to good message" $ do
            shutdown <- newEmptyMVar
            msg <- async $ replyOne shutdown

            async sendBadMsg
            rep <- async $ sendPonyMsg "\x42"
            wait rep >>= (`shouldBe` ["\x42", ""])

            putMVar shutdown ()
            wait msg >>= (`shouldBe` ("PONY", "im in ur vaults"))

        it "replies to mutliple messages" $ do
            shutdown <- newEmptyMVar
            msg <- async $ replyTwo shutdown

            rep_a <- async $ sendPonyMsg "\x43"
            rep_b <- async $ sendPonyMsg "\x44"

            wait rep_a >>= (`shouldBe` ["\x43", ""])
            wait rep_b >>= (`shouldBe` ["\x44", ""])

            putMVar shutdown ()
            wait msg >>= (`shouldBe` (("PONY", "im in ur vaults")
                                     ,("PONY", "im in ur vaults")))

    describe "Daemon day map" $ do
        it "loads an origins map" $ do
            (simple,ext) <- runTestDaemon "tcp://localhost:1234" $
                (,) <$> withSimpleDayMap "PONY" (lookupFirst 42)
                    <*> withExtendedDayMap "PONY" (lookupFirst 42)

            (,) <$> simple <*> ext `shouldBe` Just ((0, 8), (0,15))

        it "does not invalidate cache on same filesize" $ do
            result <- runTestDaemon "tcp://localhost:1234" $ do
                writePonyDayMap "02_PONY_simple_days" dayFileB
                writePonyDayMap "02_PONY_extended_days" dayFileA
                refreshOriginDays "PONY"
                withSimpleDayMap "PONY" (lookupFirst 42)

            result `shouldBe` Just (0, 8)

        it "does invalidate cache on different filesize" $ do
            result <- runTestDaemon "tcp://localhost:1234" $ do
                writePonyDayMap "02_PONY_simple_days" dayFileC
                refreshOriginDays "PONY"
                withSimpleDayMap "PONY" (lookupFirst 300)

            result `shouldBe` Just (255, 254)

            result' <- runTestDaemon "tcp://localhost:1234" $ do
                writePonyDayMap "02_PONY_extended_days" dayFileC
                refreshOriginDays "PONY"
                withExtendedDayMap "PONY" (lookupFirst 300)

            result' `shouldBe` Just (255, 254)


    describe "Daemon updateSimpleLatest" $ do
        it "does not clobber higher value" $ do
            new <- runTestDaemon "tcp://localhost:1234" $ do
                updateSimpleLatest "PONY" 0x41
                liftPool $ runObject "02_PONY_simple_latest" readFull
            new `shouldBe` Right "\x42\x00\x00\x00\x00\x00\x00\x00"

        it "does overwrite lower value" $ do
            new <- runTestDaemon "tcp://localhost:1234" $ do
                cleanup
                updateSimpleLatest "PONY" 0x43
                liftPool $ runObject "02_PONY_simple_latest" readFull
            new `shouldBe` Right "\x43\x00\x00\x00\x00\x00\x00\x00"

    describe "Daemon rollover" $  do
        it "correctly rolls over day" $ do
            new <- runTestDaemon "tcp://localhost:1234" $ do
                updateSimpleLatest "PONY" 0x42
                rollOverSimpleDay "PONY"
                liftPool $ runObject "02_PONY_simple_days" readFull
            new `shouldBe` Right dayFileD

        it "does not rollover if the day map has been touched" $ do
            new <- runTestDaemon "tcp://localhost:1234" $ do
                writePonyDayMap "02_PONY_simple_days" dayFileC

                updateSimpleLatest "PONY" 0x48
                rollOverSimpleDay "PONY"

                liftPool $ runObject "02_PONY_simple_days" readFull
            new `shouldBe` Right dayFileC

        it "does basic sanity checking on latest file" $
            runTestDaemon "tcp://localhost:1234"
                (do liftPool $ runObject "02_PONY_simple_latest" $
                        append "garbage"
                    rollOverSimpleDay "PONY")
                `shouldThrow` anyErrorCall

replyOne :: MVar () -> IO (ByteString, ByteString)
replyOne shutdown =
    runDaemon "tcp://localhost:5561" Nothing "test" $ do
        r <- reply
        liftIO $ takeMVar shutdown
        return r

reply :: Daemon (ByteString, ByteString)
reply = do
        Message rep_f (Origin origin') msg <- nextMessage
        rep_f Success
        return (origin', msg)

replyTwo :: MVar () -> IO ((ByteString, ByteString), (ByteString, ByteString))
replyTwo shutdown =
    runDaemon "tcp://localhost:5561" Nothing "test" $ do
        r <- (,) <$> reply <*> reply
        liftIO $ takeMVar shutdown
        return r

sendPonyMsg :: ByteString -> IO [ByteString]
sendPonyMsg identifier = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    -- Simulate a client sending a sequence number and message
    sendMulti s $ fromList [identifier, "PONY", "im in ur vaults"]
    receiveMulti s

sendBadMsg :: IO ()
sendBadMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    sendMulti s $ fromList ["beep"]
