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

{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings        #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module Main where

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Prelude hiding (max)

import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import Data.Int (Int64)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Word (Word32, Word64, Word8)
import Debug.Trace

--
-- What we're testing
--

import Codec.Compression.LZ4
import Control.Applicative
import Foreign.C
import Foreign.Ptr
import System.IO.Unsafe (unsafePerformIO)

import qualified Data.ByteString as S
import qualified Data.ByteString.Internal as SI
import qualified Data.ByteString.Unsafe as U


main = do
    putStrLn "Hello World"
