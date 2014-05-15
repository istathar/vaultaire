{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Control.Applicative
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.Function (on)
import Data.List (foldl', groupBy, nubBy, sort)
import Data.List
import Data.List.NonEmpty (fromList)
import Data.Monoid
import Data.Word
import System.Exit
import System.ZMQ4.Monadic
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import Test.QuickCheck.Test
import TestHelpers (runTestDaemon)
import Vaultaire.CoreTypes
import Vaultaire.Daemon
import Vaultaire.InternalStore (enumerateOrigin, readFrom, writeTo)
import Vaultaire.OriginMap

instance Arbitrary ByteString where
    arbitrary = BS.pack <$> arbitrary

main :: IO ()
main = do
    result <- quickCheckResult propWriteThenRead
    unless (isSuccess result) exitFailure

type OriginAddressKeyValues = [(ByteString, (Word64, ByteString))]

propWriteThenRead :: OriginAddressKeyValues -> Property
propWriteThenRead origins = monadicIO $ do
    let nubbed = nubBy ((==) `on` fst) origins
    origins' <- run $ runTestDaemon "tcp://localhost:1234" $ writeThenRead nubbed
    unless (origins == origins') $ run $ print (origins, origins')
    assert $ origins == origins'

writeThenRead :: OriginAddressKeyValues -> Daemon OriginAddressKeyValues
writeThenRead origins =
    forM origins $ \(origin,(k,v)) -> do
            let o = Origin origin
            let a = Address k
            writeTo o a v
            r <- readFrom o a
            case r of
                Just v' -> return (origin, (k, v'))
                Nothing -> error "no value"
