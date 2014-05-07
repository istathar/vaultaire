{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List.NonEmpty (fromList)
import System.ZMQ4.Monadic hiding (Event)
import Vaultaire.Contents

import Test.Hspec hiding (pending)
import TestHelpers

main :: IO ()
main = do
    startTestDaemons
    sendTestMsg >>= (`shouldBe` ["\x42", ""])
    hspec suite

suite :: Spec
suite = undefined
