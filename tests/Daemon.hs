{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent.Async
import Data.ByteString (ByteString)
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec
import Vaultaire.Broker
import Vaultaire.RollOver
import Vaultaire.Daemon
import Vaultaire.Util
import Control.Applicative
import System.Rados.Monadic hiding (async)

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
            runTestDaemon (return ())
            >>= (`shouldBe` ())

        it "ignores bad message and replies to good message" $ do
            msg <- async replyOne
            async sendBadMsg
            reply <- async sendTestMsg
            wait msg >>= (`shouldBe` "im in ur vaults")
            wait reply >>= (`shouldBe` ["\x42", ""])

    describe "Daemon day map" $ do
        it "loads an origins map" $ do
            result <- runTestDaemon $
                (,) <$> fetchEpoch "PONY" 42 <*> fetchNoBuckets "PONY" 42

            result `shouldBe` (Just 0, Just 8)

        it "does not invalidate cache on same filesize" $ do
            result <- runTestDaemon $ do
                writePonyDayMap dayFileB
                refreshOriginDays "PONY"
                (,) <$> fetchEpoch "PONY" 42 <*> fetchNoBuckets "PONY" 42

            result `shouldBe` (Just 0, Just 8)

        it "does invalidate cache on different filesize" $ do
            result <- runTestDaemon $ do
                writePonyDayMap dayFileC
                refreshOriginDays "PONY"
                (,) <$> fetchEpoch "PONY" 300 <*> fetchNoBuckets "PONY" 300

            result `shouldBe` (Just 255, Just 254)

    describe "Daemon updateLatest" $ do
        it "does not clobber higher value" $ do
            new <- runTestDaemon $ do
                updateLatest "PONY" 0x41
                liftPool $ runObject "02_PONY_latest" readFull
            new `shouldBe` Right "\x42\x00\x00\x00\x00\x00\x00\x00"

        it "does overwrite lower value" $ do
            new <- runTestDaemon $ do
                cleanup
                updateLatest "PONY" 0x43
                liftPool $ runObject "02_PONY_latest" readFull
            new `shouldBe` Right "\x43\x00\x00\x00\x00\x00\x00\x00"

    describe "Daemon rollover" $  do
        it "correctly rolls over day" $ do
            new <- runTestDaemon $ do
                updateLatest "PONY" 0x42
                rollOverDay "PONY"
                liftPool $ runObject "02_PONY_days" readFull
            new `shouldBe` Right dayFileD

        it "does not rollover if the day map has been touched" $ do
            new <- runTestDaemon $ do
                writePonyDayMap dayFileC

                updateLatest "PONY" 0x48
                rollOverDay "PONY"

                liftPool $ runObject "02_PONY_days" readFull
            new `shouldBe` Right dayFileC

        it "does basic sanity checking on latest file" $
            runTestDaemon
                (do liftPool $ runObject "02_PONY_latest" $ append "garbage"
                    rollOverDay "PONY")
                `shouldThrow` anyErrorCall

loadState :: Daemon ()
loadState = do
    cleanup
    writePonyDayMap dayFileA
    refreshOriginDays "PONY"
    updateLatest "PONY" 0x42

runTestDaemon :: Daemon a -> IO a
runTestDaemon a =
    runDaemon "tcp://localhost:1234" Nothing "test" (loadState >> a)

throwJust :: Monad m => Maybe RadosError -> m ()
throwJust =
    maybe (return ()) (error . show)
            
writePonyDayMap :: ByteString -> Daemon ()
writePonyDayMap contents = liftPool $ 
    runObject "02_PONY_days" (remove >> writeFull contents)
    >>= throwJust

cleanup :: Daemon ()
cleanup = liftPool $ do
    _ <- runObject "02_PONY_days" remove
    _ <- runObject "02_PONY_latest" remove
    return ()

dayFileA, dayFileB, dayFileC, dayFileD:: ByteString
dayFileA = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x08\x00\x00\x00\x00\x00\x00\x00"

dayFileB = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x0f\x00\x00\x00\x00\x00\x00\x00"

dayFileC = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x0f\x00\x00\x00\x00\x00\x00\x00\
           \\xff\x00\x00\x00\x00\x00\x00\x00\
           \\xfe\x00\x00\x00\x00\x00\x00\x00"

dayFileD = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x08\x00\x00\x00\x00\x00\x00\x00\
           \\x42\x00\x00\x00\x00\x00\x00\x00\
           \\x08\x00\x00\x00\x00\x00\x00\x00"

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
