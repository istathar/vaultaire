{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Applicative
import Control.Concurrent.MVar
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.List.NonEmpty (fromList)
import Data.Maybe
import Network.URI
import System.Rados.Monadic hiding (async)
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec

import TestHelpers
import Vaultaire.Broker
import Vaultaire.Daemon hiding (async)
import Vaultaire.DayMap
import Vaultaire.RollOver
import Vaultaire.Types
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

        it "ignores bad message and replies to good message" $
            withReplier $ do
                sendBadMsg
                sendPonyMsg >>= (`shouldBe` ["PONYim in ur vaults"])

        it "replies to mutliple messages" $
            withReplier $ do
                sendPonyMsg >>= (`shouldBe` ["PONYim in ur vaults"])
                sendPonyMsg >>= (`shouldBe` ["PONYim in ur vaults"])

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
                rollOverSimpleDay "PONY" 8
                liftPool $ runObject "02_PONY_simple_days" readFull
            new `shouldBe` Right dayFileD

        it "does not rollover if the day map has been touched" $ do
            new <- runTestDaemon "tcp://localhost:1234" $ do
                writePonyDayMap "02_PONY_simple_days" dayFileC

                updateSimpleLatest "PONY" 0x48
                rollOverSimpleDay "PONY" 8

                liftPool $ runObject "02_PONY_simple_days" readFull
            new `shouldBe` Right dayFileC

        it "does basic sanity checking on latest file" $
            runTestDaemon "tcp://localhost:1234"
                (do _ <- liftPool $ runObject "02_PONY_simple_latest" $
                            append "garbage"
                    rollOverSimpleDay "PONY" 8)
                `shouldThrow` anyErrorCall

withReplier :: IO a -> IO a
withReplier f = do
    shutdown <- newEmptyMVar
    args     <- daemonArgsDefault (fromJust $ parseURI "tcp://localhost:5561")
                                   Nothing "test" shutdown
    linkThread $ handleMessages args handler
    r <- f
    putMVar shutdown ()
    return r
  where
    handler (Message rep_f (Origin origin) msg ) =
        rep_f . PassThrough $ origin `S.append` msg


sendPonyMsg :: IO [ByteString]
sendPonyMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    -- Simulate a client sending a sequence number and message
    sendMulti s $ fromList ["PONY", "im in ur vaults"]
    receiveMulti s

sendBadMsg :: IO ()
sendBadMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    sendMulti s $ fromList ["beep"]
