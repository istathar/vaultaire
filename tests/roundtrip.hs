--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# OPTIONS -fno-warn-unused-imports #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Data.Int (Int64)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.ProtocolBuffers (encodeMessage)
import Data.Serialize
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word32, Word64)
import Debug.Trace

--
-- What we're testing
--

import Data.Time.Clock
import Data.Time.Clock.POSIX
import System.Posix.Process
import System.Rados

import Data.Locator
import Vaultaire.Conversion.Reader
import Vaultaire.Conversion.Writer
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import Vaultaire.Persistence.Constants (nanoseconds)
import qualified Vaultaire.Persistence.ContentsObject as Contents

main = do
    let s = SourceDict $ Map.fromList
           [("hostname", "secure.example.org"),
            ("metric", "eth0-tx-bytes"),
            ("datacenter", "lhr1")]

    let o' = hashStringToLocator16a 6 "arithmetic"  -- FIXME hack; we should lookup!

--
-- Use pid as a incrementing marker to make it easy to see results are sorted.
--

    now <- getPOSIXTime
    pid <- getProcessID

    let t = fromIntegral $ floor (now * 1000000000)

    let p = Point {
        origin = o',
        source = s,
        timestamp = t,
        payload = Numeric $ fromIntegral pid
--      payload = Blob $ S.pack $ show $ fromIntegral pid
    }

    let p' = encodePoint p
    let r' = createDiskPrefix (fromIntegral $ S.length p')
    let l' = Bucket.formObjectLabel o' s t

    let st = Set.singleton s

    putStrLn ""
    putStrLn $ show p
    putStrLn ""
    S.putStrLn l'
    putStrLn ""
    putStrLn $ toHex p'

    m <- runConnect (Just "vaultaire") (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            Bucket.appendVaultPoints o' [p]
            Contents.appendVaultSource o' st
            Bucket.readVaultObject o' s (timestamp p)

    putStrLn ""
    putStrLn $ show $ Map.elems m

