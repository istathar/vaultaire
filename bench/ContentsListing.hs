--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent (forkIO)
import Control.Exception (throwIO)
import Control.Monad
import Criterion.Main
import Data.ByteString (ByteString)
import Marquise.Client
import Marquise.Server
import qualified System.ZMQ4.Monadic as ZMQ
import Vaultaire.Types

contentsWire :: ByteString
contentsWire =
    "\x02\
     \\x01\x00\x00\x00\x00\x00\x00\x00\
     \\x04\x00\x00\x00\x00\x00\x00\x00\
     \\&a:b,"

runTest :: Int -> IO ()
runTest n =
    withConnection "tcp://localhost:5000" $ \c ->
        replicateM_ n (recv c >>= either throwIO nothing)
  where
    nothing :: ContentsResponse -> IO ContentsResponse
    nothing = return

server :: IO ()
server =
    ZMQ.runZMQ $ do
        s <- ZMQ.socket ZMQ.Dealer
        ZMQ.bind s "tcp://*:5000"
        forever (ZMQ.send s [] contentsWire)

main :: IO ()
main = do
    void (forkIO server)

    defaultMain
            [
              bench "contents 32" $ nfIO $ runTest 32
            , bench "contents 64" $ nfIO $ runTest 64
            , bench "contents 128" $ nfIO $ runTest 128
            , bench "contents 256" $ nfIO $ runTest 256
            , bench "contents 512" $ nfIO $ runTest 512 ]
