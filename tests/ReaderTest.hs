{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (Event)
import Test.Hspec hiding (pending)
import TestHelpers

main :: IO ()
main = do
    shutdown <- newEmptyMVar
    cleanupTestEnvironment
    startTestDaemons shutdown
    -- Prime vault by writing a test message to it
    sendTestMsg >>= (`shouldBe` ["\NUL"])

    hspec suite

    putMVar shutdown ()

suite :: Spec
suite =
    describe "full stack" $ do
        it "reads one simple message written by writer daemon" $ do
            -- Response is the data followed by an end of stream message
            let resp = ([simpleResponse], ["\x01"])
            request simpleRequest >>= (`shouldBe` resp)

        it "reads one extended message written by writer daemon" $ do
            let resp = ([extendedResponse], ["\x01"])
            request extendedRequest >>= (`shouldBe` resp)

simpleResponse :: ByteString
simpleResponse =
    "\x02" `BS.append` simpleMessage

extendedResponse :: ByteString
extendedResponse =
    "\x03" `BS.append` extendedMessage

simpleRequest :: ByteString
simpleRequest = "\x00\
                \\x04\x00\x00\x00\x00\x00\x00\x00\
                \\x00\x00\x00\x00\x00\x00\x00\x00\
                \\xff\xff\xff\xff\xff\xff\xff\xff"

extendedRequest :: ByteString
extendedRequest = "\x01\
                  \\x05\x00\x00\x00\x00\x00\x00\x00\
                  \\x00\x00\x00\x00\x00\x00\x00\x00\
                  \\xff\xff\xff\xff\xff\xff\xff\xff"

request :: ByteString -> IO ([ByteString], [ByteString])
request req = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5570"
    sendMulti s $ fromList ["PONY", req]
    rep <- receiveMulti s
    eof <- receiveMulti s
    return (rep, eof)
