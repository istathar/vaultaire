{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent.Async
import Data.ByteString (ByteString)
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec
import Vaultaire.Broker
import Vaultaire.RollOver
import Vaultaire.DayMap
import Vaultaire.Daemon hiding (async)
import Vaultaire.Util
import System.Rados.Monadic hiding (async)
import Control.Applicative

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
            wait msg >>= (`shouldBe` ("PONY", "im in ur vaults"))
            wait reply >>= (`shouldBe` ["\x42", ""])

    describe "Daemon day map" $ do
        it "loads an origins map" $ do
            (simple,ext) <- runTestDaemon $
                (,) <$> withSimpleDayMap "PONY" (lookupBoth 42)
                    <*> withExtendedDayMap "PONY" (lookupBoth 42)

            (,) <$> simple <*> ext `shouldBe` Just ((0, 8), (0,15))

        it "does not invalidate cache on same filesize" $ do
            result <- runTestDaemon $ do
                writePonyDayMap "02_PONY_simple_days" dayFileB
                writePonyDayMap "02_PONY_extended_days" dayFileA
                refreshOriginDays "PONY"
                withSimpleDayMap "PONY" (lookupBoth 42)

            result `shouldBe` Just (0, 8)

        it "does invalidate cache on different filesize" $ do
            result <- runTestDaemon $ do
                writePonyDayMap "02_PONY_simple_days" dayFileC
                refreshOriginDays "PONY"
                withSimpleDayMap "PONY" (lookupBoth 300)

            result `shouldBe` Just (255, 254)

            result' <- runTestDaemon $ do
                writePonyDayMap "02_PONY_extended_days" dayFileC
                refreshOriginDays "PONY"
                withExtendedDayMap "PONY" (lookupBoth 300)

            result' `shouldBe` Just (255, 254)


    describe "Daemon updateSimpleLatest" $ do
        it "does not clobber higher value" $ do
            new <- runTestDaemon $ do
                updateSimpleLatest "PONY" 0x41
                liftPool $ runObject "02_PONY_simple_latest" readFull
            new `shouldBe` Right "\x42\x00\x00\x00\x00\x00\x00\x00"

        it "does overwrite lower value" $ do
            new <- runTestDaemon $ do
                cleanup
                updateSimpleLatest "PONY" 0x43
                liftPool $ runObject "02_PONY_simple_latest" readFull
            new `shouldBe` Right "\x43\x00\x00\x00\x00\x00\x00\x00"

    describe "Daemon rollover" $  do
        it "correctly rolls over day" $ do
            new <- runTestDaemon $ do
                updateSimpleLatest "PONY" 0x42
                rollOverSimpleDay "PONY"
                liftPool $ runObject "02_PONY_simple_days" readFull
            new `shouldBe` Right dayFileD

        it "does not rollover if the day map has been touched" $ do
            new <- runTestDaemon $ do
                writePonyDayMap "02_PONY_simple_days" dayFileC

                updateSimpleLatest "PONY" 0x48
                rollOverSimpleDay "PONY"

                liftPool $ runObject "02_PONY_simple_days" readFull
            new `shouldBe` Right dayFileC

        it "does basic sanity checking on latest file" $
            runTestDaemon
                (do liftPool $ runObject "02_PONY_simple_latest" $
                        append "garbage"
                    rollOverSimpleDay "PONY")
                `shouldThrow` anyErrorCall

loadState :: Daemon ()
loadState = do
    cleanup
    writePonyDayMap "02_PONY_simple_days" dayFileA
    writePonyDayMap "02_PONY_extended_days" dayFileB
    refreshOriginDays "PONY"
    updateSimpleLatest "PONY" 0x42
    updateExtendedLatest "PONY" 0x52

runTestDaemon :: Daemon a -> IO a
runTestDaemon a =
    runDaemon "tcp://localhost:1234" Nothing "test" (loadState >> a)

throwJust :: Monad m => Maybe RadosError -> m ()
throwJust =
    maybe (return ()) (error . show)
            
writePonyDayMap :: ByteString -> ByteString -> Daemon ()
writePonyDayMap oid contents = liftPool $ 
    runObject oid (writeFull contents)
    >>= throwJust

cleanup :: Daemon ()
cleanup = liftPool $ do
    runObject "02_PONY_simple_days" remove
    runObject "02_PONY_extended_days" remove
    runObject "02_PONY_simple_latest" remove
    runObject "02_PONY_extended_latest" remove
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

replyOne :: IO (ByteString, ByteString)
replyOne =
    runDaemon "tcp://localhost:5561" Nothing "test" $ do
        Message rep_f origin' msg <- nextMessage
        rep_f Success
        return (origin', msg)

sendTestMsg :: IO [ByteString]
sendTestMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    -- Simulate a client sending a sequence number and message
    sendMulti s $ fromList ["\x42", "PONY", "im in ur vaults"]
    receiveMulti s

sendBadMsg :: IO ()
sendBadMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5560"
    sendMulti s $ fromList ["beep"]
