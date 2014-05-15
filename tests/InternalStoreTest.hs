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
<<<<<<< HEAD
import TestHelpers(runTestDaemon)
=======
import TestHelpers
import Vaultaire.CoreTypes
>>>>>>> 816f8827a8f01cd120f088e720c853846b6401ef
import Vaultaire.InternalStore(writeTo, readFrom, enumerateOrigin)
import Vaultaire.Daemon
import Vaultaire.OriginMap
import Vaultaire.CoreTypes
import Data.List
import Data.Function(on)

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
<<<<<<< HEAD
            let a = Address k
            writeTo o a v
            r <- readFrom o a
            case r of
                Just v' -> return (origin, (k, v'))
                Nothing -> error "no value"
=======
            writeTo o (Address k) v >>= either error return
            r <- readFrom o (Address k)
            either error (return . (k,)) r)
>>>>>>> 816f8827a8f01cd120f088e720c853846b6401ef
