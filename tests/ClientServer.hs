{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Concurrent.Async
import Marquise.Client
import Marquise.IO
import Test.Hspec
import Vaultaire.Daemon hiding (async)
import Vaultaire.Broker
import Vaultaire.CoreTypes
import Data.ByteString(ByteString)
import Control.Concurrent
import Vaultaire.Util
import System.ZMQ4.Monadic hiding (async)

ns1, ns2, ns3 :: NameSpace
ns1 = either error id $ mkNameSpace "ns1"
ns2 = either error id $ mkNameSpace "ns2"
ns3 = either error id $ mkNameSpace "ns3"

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

        it "talks to a vaultaire daemon" $ do
            shutdown <- newEmptyMVar
            msg <- async (reply shutdown)
                
            transmitBytes "tcp://localhost:5560" "PONY" "bytes"
            putMVar shutdown ()
            wait msg >>= (`shouldBe` ("PONY", "bytes"))

reply :: MVar () -> IO (ByteString, ByteString)
reply shutdown = 
    runDaemon "tcp://localhost:5561" Nothing "test" $ do
        Message rep_f (Origin origin') msg <- nextMessage
        rep_f Success
        liftIO $ takeMVar shutdown
        return (origin', msg)
