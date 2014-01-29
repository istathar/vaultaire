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
{-# LANGUAGE PackageImports     #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module Main where

import Codec.Compression.LZ4
import Control.Applicative
import Control.Monad (forever)
import "mtl" Control.Monad.Error ()
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock
import System.Environment (getArgs, getProgName)
import System.Rados
import System.ZMQ4.Monadic hiding (source)

import Vaultaire.Conversion.Receiver
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents

main :: IO ()
main = do
    args <- getArgs

    let worker = if length args == 1
                    then head args
                    else error "Specify broker hostname or IP address on command line"


    runZMQ $ do
        telem <- socket Sub
        connect telem  ("tcp://" ++ worker ++ ":5570")
        subscribe telem ""

        loop telem
  where
        loop telem = do
            message' <- receive telem

            liftIO $ S.putStrLn message'
