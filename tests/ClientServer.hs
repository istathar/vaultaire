{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Concurrent
import Control.Concurrent.Async
import Data.ByteString (ByteString)
import Marquise.Classes
import Marquise.Client
import Marquise.IO ()
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec
import Vaultaire.Broker
import Vaultaire.Daemon hiding (async)
import Vaultaire.Types
import Vaultaire.Util
import Control.Exception(throw)

ns1, ns2 :: SpoolName
ns1 = either throw id $ makeSpoolName "ns1"
ns2 = either throw id $ makeSpoolName "ns2"

main :: IO ()
main = do
    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
    hspec suite

suite :: Spec
suite =
    describe "IO MarquiseClientMonad and MarquiseServerMonad" $ do
        it "reads appends, then cleans up when nextBurst is called" $ do
            sf1 <- createSpoolFiles "ns1"
            sf2 <- createSpoolFiles "ns2"

            appendPoints sf1 "BBBBBBBBAAAAAAAACCCCCCCC"
            appendPoints sf1 "DBBBBBBBAAAAAAAACCCCCCCC"
            appendPoints sf2 "FBBBBBBBAAAAAAAACCCCCCCC"
            appendContents sf1 "contents"

            (bytes1,close_f1) <- nextPoints ns1
            (bytes2,close_f2) <- nextPoints ns2
            (contents,close_f3) <- nextContents ns1

            bytes1 `shouldBe` "BBBBBBBBAAAAAAAACCCCCCCC\
                              \DBBBBBBBAAAAAAAACCCCCCCC"
            bytes2 `shouldBe` "FBBBBBBBAAAAAAAACCCCCCCC"
            close_f1
            close_f2

            contents `shouldBe` "contents"
            close_f3


        it "talks to a vaultaire daemon" $ do
            shutdown <- newEmptyMVar
            msg <- async (reply shutdown)

            transmitBytes "localhost" "PONY" "bytes"
            putMVar shutdown ()
            wait msg >>= (`shouldBe` ("PONY", "bytes"))

reply :: MVar () -> IO (ByteString, ByteString)
reply shutdown =
    runDaemon "tcp://localhost:5561" Nothing "test" $ do
        Message rep_f (Origin origin') msg <- nextMessage
        rep_f OnDisk
        liftIO $ takeMVar shutdown
        return (origin', msg)
