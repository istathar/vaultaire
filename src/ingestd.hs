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
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Time.Clock
import System.Environment (getArgs, getProgName)
import System.Rados
import System.ZMQ4.Monadic hiding (source)
import Text.Groom

import Vaultaire.Conversion.Receiver
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents




{-
groupBurst :: [Point] -> Either String (Map Origin [Point])
groupBurst [] = Left "Zero length burst, ignoring"
groupBurst ps =
    Right $ foldl f Map.empty ps
  where
    f :: Map Origin [Point] -> Point -> Map Origin [Point]
    f m p = Map.insertWith (++) (origin p) [p] m
-}

--
-- This will be refactored since the Origin value will soon be conveyed at
-- the ØMQ level, rather than the current hack of an environment variable
-- passed to libmarquise. But at the moment this is a simple enough check
-- to ensure we at least have a value.
--

sanityCheck :: [Point] -> Either String [Point]
sanityCheck ps =
  let
    o' = origin $ head ps
  in
    if S.null o'
        then Left "Empty origin value, discarding burst"
        else Right ps


processBurst :: Map Origin (Set SourceDict) -> Origin -> [Point] -> IO (Set SourceDict)
processBurst _ _ [] =
    return Set.empty
processBurst cm o' ps =
  let
    known = Map.findWithDefault Set.empty o' cm

    new :: Set SourceDict
    new = foldl g Set.empty ps

    g :: Set SourceDict -> Point -> Set SourceDict
    g st p =
      let
        s = source p
      in
        if Set.member s known
            then st
            else Set.insert s st

  in do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            let l' = Contents.formObjectLabel o'
            withSharedLock l' "name" "desc" "tag" (Just 10.0) $ do

                -- returns the sources that are "new"
                Bucket.appendVaultPoints o' ps
    return new


parseMessage :: ByteString -> Either String [Point]
parseMessage message' = do
    y' <- case decompress message' of
        Just x' -> Right x'
        Nothing -> Left "Decompressing DataBurst failed"

    ps <- decodeBurst y'

    sanityCheck ps


--
-- This takes *a* contents list, not *the* contents list, in other words
-- this is just conveying the SourceDicts that are "new".
--
updateContents :: Map Origin (Set SourceDict) -> Origin -> Set SourceDict -> IO (Map Origin (Set SourceDict))
updateContents cm0 o' new =
  let
    orig = Map.findWithDefault Set.empty o' cm0

    replace = Set.foldl (\acc s -> Set.insert s acc) orig new
    cm1 = Map.insert o' replace cm0
    l' = Contents.formObjectLabel o'
  in do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test1" $ do
            withSharedLock l' "name" "desc" "tag" (Just 10.0) $ do
                Contents.appendVaultSource l' new
    return cm1


debugTime :: UTCTime -> IO ()
debugTime t1 = do
    t2 <- getCurrentTime
    debug $ diffUTCTime t2 t1


debug :: Show σ => σ -> IO ()
debug x = putStrLn $ show x


main :: IO ()
main = do
    args <- getArgs

    let broker = if length args == 1
                    then head args
                    else error "Specify broker hostname or IP address on command line"

    cm <- return $ Map.singleton "FIXME0" Set.empty

    runZMQ $ do
        pull <- socket Pull
        connect pull ("tcp://" ++ broker ++ ":5561")

        ack  <- socket Push
        connect ack  ("tcp://" ++ broker ++ ":5560")

        loop pull ack cm
  where
        loop pull ack cm = do
            [envelope', delimiter', message'] <- receiveMulti pull
            t <- liftIO $ getCurrentTime

            (ok', o', st) <- liftIO $ case parseMessage message' of
                Left err -> do
                    -- temporary, replace with telemetry
                    debug err

                    return $ (S.pack err, S.empty, Set.empty)
                Right ps -> do
                    -- temporary, replace with zmq message part
                    let o' = origin $ head ps

                    st <- processBurst cm o' ps

                    -- temporary, replace with telemetry
                    debug $ length ps

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

            cm2 <- liftIO $ updateContents cm o' st
            liftIO $ debugTime t

            loop pull ack cm2

