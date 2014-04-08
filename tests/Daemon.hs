{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent.Async
import Data.ByteString
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec
import Vaultaire.Broker
import Vaultaire.Daemon
import Vaultaire.Util

main :: IO ()
main = do
    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"

    hspec suite

-- | A pre-requisite for this test suite is a connection to a test ceph cluster
-- with a "test" pool.
suite :: Spec
suite =
    describe "Daemon" $ do
        it "starts up and shuts down cleanly" $
            runDaemon "tcp://localhost:1234" Nothing "test" (return ())
            >>= (`shouldBe` ())

        it "ignores bad message and replies to good message" $ do
            msg <- async replyOne
            async sendBadMsg
            reply <- async sendTestMsg
            wait msg >>= (`shouldBe` "im in ur vaults")
            wait reply >>= (`shouldBe` ["\x42", ""])


replyOne :: IO ByteString
replyOne =
    runDaemon "tcp://localhost:5561" Nothing "test" $ do
        Message rep_f msg <- nextMessage
        rep_f Success
        return msg

sendTestMsg :: IO [ByteString]
sendTestMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    -- Simulate a client sending a sequence number and message
    sendMulti s $ fromList ["\x42", "im in ur vaults"]
    receiveMulti s

sendBadMsg :: IO ()
sendBadMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    sendMulti s $ fromList ["beep"]
