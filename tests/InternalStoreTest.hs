{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections   #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.List (foldl', groupBy, nubBy, sort)
import Data.List.NonEmpty (fromList)
import Data.Monoid
import Data.Word
import System.Exit
import System.ZMQ4.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import Test.QuickCheck.Test
import TestHelpers
import Vaultaire.CoreTypes
import Vaultaire.InternalStore(writeTo, readFrom, enumerateOrigin)
import Vaultaire.Daemon
import Vaultaire.OriginMap

instance Arbitrary ByteString where
    arbitrary = BS.pack <$> arbitrary

main :: IO ()
main = do
    result <- verboseCheckResult propWriteThenRead
    unless (isSuccess result) exitFailure

type OriginAddressKeyValues = [(ByteString, [(Word64, ByteString)])] 

propWriteThenRead :: OriginAddressKeyValues -> Property
propWriteThenRead origins = monadicIO $ do
    origins' <- run $ runTestDaemon "tcp://localhost:1234" $ writeThenRead origins
    assert $ origins == origins'

writeThenRead :: OriginAddressKeyValues -> Daemon OriginAddressKeyValues
writeThenRead origins =
    forM origins $ \(origin,kvs) ->
        (origin,) <$> forM kvs (\(k, v) -> do
            let o = Origin origin
            writeTo o (Address k) v >>= either error return
            r <- readFrom o (Address k)
            either error (return . (k,)) r)
