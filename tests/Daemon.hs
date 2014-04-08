{-# LANGUAGE OverloadedStrings #-}

module Main where

import Test.Hspec
import Vaultaire.Daemon

main :: IO ()
main =
    hspec suite

suite :: Spec
suite =
    describe "Daemon" $
        it "starts up and shuts down cleanly" $
            runDaemon "broker" Nothing "pool" (return ()) >>= (`shouldBe` ())
