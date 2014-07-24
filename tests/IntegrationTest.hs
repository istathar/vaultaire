--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where


import Control.Concurrent
import Control.Monad
import qualified Data.ByteString.Char8 as S
import Data.HashMap.Strict (fromList)
import Data.Text (Text)
import Data.Word
import Pipes
import qualified Pipes.Prelude as P
import System.IO.Unsafe
import Test.Hspec hiding (pending)

import CommandRunners
import DaemonRunners
import Marquise.Client
import TestHelpers (cleanup)
import Vaultaire.Daemon
import Vaultaire.Types (getCurrentTimeNanoseconds)

pool :: String
pool = "test"

user :: String
user = "vaultaire"

namespace :: String
namespace = "integration"

now :: Word64
now = unTimeStamp $ unsafePerformIO $ getCurrentTimeNanoseconds
{-# NOINLINE now #-}

-- FIXME needs to erase marquise spool as well
destroyExistingVault :: IO ()
destroyExistingVault =
    runDaemon "inproc://1" (Just (S.pack user)) (S.pack pool) cleanup

startServerDaemons :: MVar () -> IO ()
startServerDaemons shutdown =
  let
    broker = "localhost"
    bucket_size = 256
    num_buckets = 128
    step_size = 1
    origin = Origin "ZZZZZZ"
  in do
    runBrokerDaemon shutdown
    runWriterDaemon pool user broker bucket_size shutdown
    runReaderDaemon pool user broker shutdown
    runContentsDaemon pool user broker shutdown
    runMarquiseDaemon broker origin namespace shutdown
    junk <- newEmptyMVar -- TODO get the MVar out of commands
    runRegisterOrigin pool user origin num_buckets step_size 0 0 junk

setupClientSide :: IO SpoolFiles
setupClientSide = do
    createSpoolFiles namespace


main :: IO ()
main = do
    quit <- newEmptyMVar

    destroyExistingVault
    startServerDaemons quit

    spool <- setupClientSide

    hspec (suite spool)

    putMVar quit ()


suite :: SpoolFiles -> Spec
suite spool =
  let
    origin    = Origin "ZZZZZZ"
    delay     = 10000   -- 10 ms
    maxReads  = 100
  in do
    describe "Single fixed data point" $
      let
        address   = hashIdentifier "Row row row yer boat"
        begin     = 1406078299651575183
        end       = 1406078299651575183
        timestamp = 1406078299651575183
        payload   = 42
      in do
        it "sends point, via marquise" $ do
            queueSimple spool address timestamp payload
            flush spool
            pass

        it "reads point, via marquise" $
          let
            go n = do
                result <- withReaderConnection "localhost" $ \c -> do
                    P.head (readSimple address begin end origin c >-> decodeSimple)

                case result of
                    Nothing -> if n > 100
                                then expectationFailure ("Expected a value back, didn't get one after " ++ show (n * delay `div` 1000) ++ " ms")
                                else do
                                    threadDelay delay
                                    go (n+1)
                    Just v  -> (simplePayload v) `shouldBe` payload
          in
            go 1

    describe "Data crosses rollover boundary" $
      let
        address = hashIdentifier "Gently down the stream"
        count   = 100
        f x     = (address, TimeStamp (begin+x), x)
        begin   = now
        end     = begin + count
        points  = map f [1..count]
      in do
        it "generates incrementing data stream" $ do
            forM_ points $ \(a,t,v) -> do
                queueSimple spool a t v
            flush spool
            pass

        it "retreives incrementing data stream" $
          let
            go n = do
                result <- withReaderConnection "localhost" $ \c -> do
                    P.toListM (readSimple address begin end origin c >-> decodeSimple)

                case result of
                    []      -> if n >= maxReads
                                then expectationFailure ("Expected a value back, didn't get one after " ++ show (n * delay `div` 1000) ++ " ms")
                                else do
                                    threadDelay delay
                                    go (n+1)
                    _       -> map (\p -> (simpleAddress p, TimeStamp $ simpleTime p, simplePayload p)) result
                                `shouldBe` points
          in
            go 0





-- | Mark that we are expecting this code to have succeeded, unless it threw an exception
pass :: Expectation
pass = return ()

listToDict :: [(Text, Text)] -> SourceDict
listToDict elts = either error id . makeSourceDict $ fromList elts

