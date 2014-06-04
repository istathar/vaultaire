{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (Event)
import Test.Hspec hiding (pending)
import TestHelpers
import Vaultaire.Reader

main :: IO ()
main = do
    runTestDaemon "tcp://localhost:1234" (return ())
    startTestDaemons
    -- Prime vault by writing a test message to it
    sendTestMsg >>= (`shouldBe` ["\x42", "\NUL"])
    hspec suite

suite :: Spec
suite = do
    describe "message classification" $ do
        it "correctly classifies simple message" $
            case classifyPayload simpleRequest of
                SomeRequest (Simple _) -> ()
                _  -> error "not simple"
            `shouldBe` ()

        it "correctly classifies extended message" $
            case classifyPayload extendedRequest of
                SomeRequest (Extended _) -> ()
                _  -> error "not extended"
            `shouldBe` ()

        it "correctly classifies invalid message" $
            case classifyPayload "" of
                SomeRequest (Invalid _) -> ()
                _  -> error "not invalid"
            `shouldBe` ()

    describe "full stack" $ do
        it "reads one simple message written by writer daemon" $ do
            -- Response is the data followed by an end of stream message
            let resp = (["\x43", simpleResponse], ["\x43", "\x01"])
            request simpleRequest >>= (`shouldBe` resp)

        it "reads one extended message written by writer daemon" $ do
            let resp = (["\x43", extendedResponse], ["\x43", "\x01"])
            request extendedRequest >>= (`shouldBe` resp)

simpleResponse :: ByteString
simpleResponse =
    "\x02" `BS.append` simpleMessage

extendedResponse :: ByteString
extendedResponse =
    "\x03" `BS.append` extendedMessage

simpleRequest :: ByteString
simpleRequest = "\x00\x00\x00\x00\x00\x00\x00\x00\
                \\x04\x00\x00\x00\x00\x00\x00\x00\
                \\x00\x00\x00\x00\x00\x00\x00\x00\
                \\xff\xff\xff\xff\xff\xff\xff\xff"

extendedRequest :: ByteString
extendedRequest = "\x00\x00\x00\x00\x00\x00\x00\x00\
                  \\x05\x00\x00\x00\x00\x00\x00\x00\
                  \\x00\x00\x00\x00\x00\x00\x00\x00\
                  \\xff\xff\xff\xff\xff\xff\xff\xff"

request :: ByteString -> IO ([ByteString], [ByteString])
request req = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5570"
    sendMulti s $ fromList ["\x43", "PONY", req]
    rep <- receiveMulti s
    eof <- receiveMulti s
    return (rep, eof)
