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
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word32, Word64)
import Debug.Trace
import Numeric (showHex)
import Text.Groom
import Text.Printf

--
-- What we're testing
--

import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.ObjectFormat as Bucket

main = do
    let tags = Map.fromList
           [("hostname", "secure.example.org"),
            ("metric", "eth0-tx-bytes"),
            ("datacenter", "lhr1"),
            ("epoch", "1")]

    let p = Point {
        origin = "perf_data",
        source = tags,
        timestamp = 1386931666289201468,
        payload = Numeric 201468
--      payload = Textual "66.249.74.101 - - [12/Nov/2013:04:02:20 +1100] \"GET /the-politics-of-praise-william-w-young-iii/prod9780754656463.html HTTP/1.1\" 200 15695 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""
--      payload = Measurement 45.9
--      payload = Blob (B.pack [0x01, 0x0f, 0x5a])
    }

    let pb = createDataFrame p

    putStrLn ""
    putStrLn $ groom p
    putStrLn ""
    putStrLn $ show pb
    putStrLn ""

    let p' = runPut $ encodeMessage pb
    putStrLn $ "0x" ++ toHex p'

    putStrLn ""
    S.putStrLn $ Bucket.formObjectLabel p


toHex :: ByteString -> String
toHex = concat . map (printf "%02X") . B.unpack

