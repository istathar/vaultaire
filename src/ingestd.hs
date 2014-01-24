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

import Codec.Compression.LZ4
import Control.Applicative
import Control.Monad (forever)
import Control.Monad.Error
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import Data.Map (Map)
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import System.Environment (getArgs, getProgName)
import System.Rados
import System.ZMQ4.Monadic
import Text.Groom

import Vaultaire.Conversion.Receiver
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents


groupBurst :: [Point] -> Either String (Map Origin [Point])
groupBurst [] = Left "Zero length burst, ignoring"
groupBurst ps =
    Right $ foldl f Map.empty ps
  where
    f :: Map Origin [Point] -> Point -> Map Origin [Point]
    f m p = Map.insertWith (++) (origin p) [p] m

{-

  let
    o' = origin p
  in
    if S.null o'
        then Left "Empty origin value, discarding burst"
        else Right ps
-}

processBurst :: [Point] -> IO (Origin, Set SourceDict)
processBurst [] = return (S.empty, Set.empty)
processBurst ps =
  let
    o' = origin $ head ps
  in do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            let l' = Contents.formObjectLabel o'
            withSharedLock l' "name" "desc" "tag" (Just 10.0) $ do

                -- returns the sources that are "new"
                st <- Bucket.appendVaultPoints o' ps
                return (o',st)


parseMessage :: ByteString -> Either String [Point]
parseMessage message' = do
    y' <- case decompress message' of
        Just x' -> Right x'
        Nothing -> Left "Decompressing DataBurst failed"

    ps <- decodeBurst y'
    return ps

--          groupBurst ps


updateContents :: Origin -> Set SourceDict -> IO ()
updateContents o' st = do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            let l' = Contents.formObjectLabel o'
            withSharedLock l' "name" "desc" "tag" (Just 10.0) $ do
                Contents.appendVaultSource l' st


main :: IO ()
main = do
    args <- getArgs

    let broker = if length args == 1
                    then head args
                    else error "Specify broker hostname or IP address on command line"

    runZMQ $ do
        pull <- socket Pull
        connect pull ("tcp://" ++ broker ++ ":5561")

        ack  <- socket Push
        connect ack  ("tcp://" ++ broker ++ ":5560")

        forever $ do
            [envelope', delimiter', message'] <- receiveMulti pull

            (ok', o', st) <- liftIO $ case parseMessage message' of
                    Left err -> do
                        putStr err                  -- temporary
                        return $ (S.pack err, S.empty, Set.empty)
                    Right ps -> do
                        (o',st) <- processBurst ps
                        print $ length ps         -- tempoary
                        return $ (S.empty, o', st)

--
-- We have to use sendMulti because we are manually following the rules of
-- DEALER/ROUTER sockets which send along (one or more) "envelope" messages in
-- a multipart so that the downstream knows where to send acknowledgements to; you
-- return the
--

            sendMulti ack (fromList [envelope', delimiter', ok'])

--
-- Now, before looping, write updates to the contents list, if any.
--

            liftIO $ updateContents o' st

