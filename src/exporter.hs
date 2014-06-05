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

{-# LANGUAGE CPP                #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PackageImports     #-}
{-# LANGUAGE RecordWildCards    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where

import Codec.Compression.LZ4
import Control.Applicative
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Foldable (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Time.Clock
import System.Environment (getArgs)
import qualified System.Rados.Monadic as Rados
import Text.Printf

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import Data.Word
import qualified Vaultaire.Persistence.ContentsObject as Contents

import qualified "vaultaire" Marquise.Client as Marquise
import qualified "vaultaire" Vaultaire.Types as Vaultaire

hashSourceToAddress :: String -> Word64
hashSourceToAddress = Marquise.hashIdentifier . S.pack

filterUndesireables :: SourceDict -> SourceDict
filterUndesireables = Map.delete "origin" . Map.delete "uom"

main :: IO ()
main = do
    -- just one
    [origin] <- getArgs

    let Right spool = Marquise.makeSpoolName "exporter"

    Rados.runConnect (Just "vaultaire") (Rados.parseConfig "/etc/ceph/ceph.conf") $ do
        Rados.runPool "vaultaire" $ do
            let o = S.pack origin
            let l = Contents.formObjectLabel o
            st <- Contents.readVaultObject l
            let t1 = 1393632000 --  1 March
            let t2 = 1402790400 -- 15 June
            let is = Bucket.calculateTimemarks t1 t2
           
            -- TODO register contents

            forM_ st $ \s -> do
                forM_ is $ \i -> do
                    m <- Bucket.readVaultObject o s i

                    unless (Map.null m) $ do
                        let ps = Bucket.pointsInRange t1 t2 m
                        
                        forM_ ps $ \p -> do
                            let a = (hashSourceToAddress . show . filterUndesireables) s
                            Marquise.withBroker "nebula" $ do
                                Marquise.updateSourceDict a s



