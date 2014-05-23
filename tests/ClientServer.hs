{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Concurrent
import Control.Concurrent.Async
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BS
import Marquise.Client
import Marquise.IO
import System.ZMQ4.Monadic hiding (async)
import Test.Hspec
import Vaultaire.Broker
import Vaultaire.CoreTypes
import Vaultaire.Daemon hiding (async)
import Vaultaire.Util

ns1, ns2, ns3 :: NameSpace
ns1 = either error id $ makeNameSpace "ns1"
ns2 = either error id $ makeNameSpace "ns2"
ns3 = either error id $ makeNameSpace "ns3"

main :: IO ()
main = do
    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
    hspec suite

suite :: Spec
suite =
    describe "IO MarquiseClientMonad and MarquiseServerMonad" $ do
        it "returns nothing on empty file" $
            nextBurst ns3 >>= (`shouldBe` Nothing)

        it "reads two appends, then cleans up when nextBurst is called" $ do
            append ns1 "BBBBBBBBAAAAAAAACCCCCCCC"
            append ns1 "DBBBBBBBAAAAAAAACCCCCCCC"
            append ns2 "FBBBBBBBAAAAAAAACCCCCCCC"

            Just (bp1,bytes1) <- nextBurst ns1
            Just (bp2,bytes2) <- nextBurst ns2

            bytes1 `shouldBe` "BBBBBBBBAAAAAAAACCCCCCCC\
                              \DBBBBBBBAAAAAAAACCCCCCCC"
            bytes2 `shouldBe` "FBBBBBBBAAAAAAAACCCCCCCC"

            flagSent bp1
            flagSent bp2

            nextBurst ns1 >>= (`shouldBe` Nothing)
            nextBurst ns2 >>= (`shouldBe` Nothing)

        it "splits up after 1MB" $ do
            let large_burst = BS.replicate 1048584 'B'
            append ns1 large_burst
            append ns1 "DDDDDDDDDDDDDDDDDDDDDDDD"
            Just (bp1,bytes1) <- nextBurst ns1
            Just (bp2,bytes2) <- nextBurst ns1
            nextBurst ns1 >>= (`shouldBe` Nothing)

            (bp1 == bp2) `shouldBe` False
            bytes1 `shouldBe` BS.toStrict large_burst
            bytes2 `shouldBe` "DDDDDDDDDDDDDDDDDDDDDDDD"

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
        rep_f Success
        liftIO $ takeMVar shutdown
        return (origin', msg)
