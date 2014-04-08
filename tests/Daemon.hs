{-# LANGUAGE OverloadedStrings #-}

module Main where

import Test.Hspec
import Vaultaire.Daemon
import Vaultaire.Broker
import System.ZMQ4.Monadic (runZMQ, Router(..), Dealer(..))
import Control.Concurrent

main :: IO ()
main = do
    forkIO $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"

    hspec suite

suite :: Spec
suite =
    describe "Daemon" $
        it "starts up and shuts down cleanly" $
            runDaemon "tcp://localhost:5560" Nothing "test" (return ())
            >>= (`shouldBe` ())
