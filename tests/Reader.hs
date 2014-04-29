{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}

module Main where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (Event)
import Test.Hspec hiding (pending)
import TestHelpers
import Vaultaire.Broker
import Vaultaire.Util
import Vaultaire.Writer
import Vaultaire.Reader

main :: IO ()
main = do
    runTestDaemon "tcp://localhost:1234" (return ())
    startTestDaemons
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
            requestSimpleTestMsg >>= (`shouldBe` ["\x43", simpleResponse])

        it "reads one extended message written by writer daemon" $ do
            requestExtendedTestMsg >>= (`shouldBe` ["\x44", extendedResponse])

startTestDaemons :: IO ()
startTestDaemons = do
    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5570") (Dealer,"tcp://*:5571") "tcp://*:5001"
    linkThread $ startWriter "tcp://localhost:5561" Nothing "test" 0
    linkThread $ startReader "tcp://localhost:5571" Nothing "test"
    sendTestMsg >>= (`shouldBe` ["\x42", ""])

simpleResponse :: ByteString
simpleResponse =
    "\x02\x00\x00\x00\x00\x00\x00\x00" `BS.append` simpleMessage

extendedResponse :: ByteString
extendedResponse =
    "\x02\x00\x00\x00\x00\x00\x00\x00" `BS.append` extendedMessage

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

requestSimpleTestMsg :: IO [ByteString]
requestSimpleTestMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5570"
    sendMulti s $ fromList ["\x43", "PONY", simpleRequest]
    receiveMulti s

requestExtendedTestMsg :: IO [ByteString]
requestExtendedTestMsg = runZMQ $ do
    s <- socket Dealer
    connect s "tcp://localhost:5570"
    sendMulti s $ fromList ["\x44", "PONY", extendedRequest]
    receiveMulti s
