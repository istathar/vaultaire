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

processBurst :: Map Origin [Point] -> IO ()
processBurst m = void $ do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            Map.traverseWithKey (\o' ps -> do
                let l' = Contents.formObjectLabel o'
                withSharedLock l' "name" "desc" "tag" (Just 10.0) $
                    Bucket.appendVaultPoints o' ps) m


parseMessage :: ByteString -> Either String (Map Origin [Point])
parseMessage message' = do
            y' <- case decompress message' of
                Just x' -> Right x'
                Nothing -> Left "Decompressing DataBurst failed"

            ps <- decodeBurst y'

            groupBurst ps




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

            ok' <- case parseMessage message' of
                    Left err -> do
                        liftIO $ putStr err
                        return $ S.pack err
                    Right mp -> do
                        liftIO $ processBurst mp
                        liftIO $ print $ Map.size mp
                        return $ S.empty

--
-- We have to use sendMulti because we are manually following the rules of
-- DEALER/ROUTER sockets which send along (one or more) "envelope" messages in
-- a multipart so that the downstream knows where to send acknowledgements to; you
-- return the
--

            sendMulti ack (fromList [envelope', delimiter', ok'])

