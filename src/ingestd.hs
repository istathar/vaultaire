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


{-
groupBurst :: [Point] -> Either String (Map Origin [Point])
groupBurst [] = Left "Zero length burst, ignoring"
groupBurst ps =
    Right $ foldl f Map.empty ps
  where
    f :: Map Origin [Point] -> Point -> Map Origin [Point]
    f m p = Map.insertWith (\(x:[]) xs -> (x:xs)) (origin p) [p] m
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


processBurst
    :: Map Origin (Set SourceDict)
    -> Origin
    -> [Point]
    -> ByteString
    -> IO (Set SourceDict)
processBurst _ _ [] _ =
    return Set.empty
processBurst cm o' ps pool' =
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
        runPool pool' $ do
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
updateContents
    :: Map Origin (Set SourceDict)
    -> Origin
    -> Set SourceDict
    -> ByteString
    -> IO (Map Origin (Set SourceDict))
updateContents cm0 o' new pool' =
  let
    st0 = Map.findWithDefault Set.empty o' cm0

    l' = Contents.formObjectLabel o'
  in do
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool pool' $ do
            withSharedLock l' "name" "desc" "tag" (Just 10.0) $ do
                st1 <- if Set.null st0
                    then do
                        Contents.readVaultObject l'
                    else do
                        return st0

                let st2 = Set.foldl (\acc s -> Set.insert s acc) st1 new

                if Set.size st2 > Set.size st1
                    then do
                        Contents.appendVaultSource l' new
                        return $ Map.insert o' st2 cm0
                    else
                        return cm0


debug :: Show σ => σ -> IO ()
debug x = putStrLn $ show x


main :: IO ()
main = do
    args <- getArgs

    let [broker,pool] = if length args == 2
                    then take 2 args
                    else error "usage: ingestd broker poolname"

    let cm = Map.empty

    runZMQ $ do
        pull <- socket Pull
        connect pull ("tcp://" ++ broker ++ ":5561")

        ack  <- socket Push
        connect ack  ("tcp://" ++ broker ++ ":5560")

        telem <- socket Pub
        bind telem "tcp://*:5570"

        loop pull ack telem cm (S.pack pool)
  where
        loop pull ack telem cm pool' = do
            [envelope', delimiter', message'] <- receiveMulti pull
            t1 <- liftIO $ getCurrentTime

            (ok', o', st, num) <- liftIO $ case parseMessage message' of
                Left err -> do
                    -- temporary, replace with telemetry
                    debug err

                    return $ (S.pack err, S.empty, Set.empty, 0)
                Right ps -> do
                    -- temporary, replace with zmq message part
                    let o' = origin $ head ps

                    st <- processBurst cm o' ps pool'

                    return $ (S.empty, o', st, length ps)

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

            cm2 <- if Set.null st
                then return cm
                else liftIO $ updateContents cm o' st pool'

            t2 <- liftIO $ getCurrentTime
            let delta = diffUTCTime t2 t1
            send telem [] $ composeTelemetry delta num message'

            loop pull ack telem cm2 pool'


composeTelemetry :: NominalDiffTime -> Int -> ByteString -> ByteString
composeTelemetry delta num message' =
    S.intercalate " " [delta', num', size']
  where
    delta' = S.pack $ show delta
    num'   = S.pack $ show num
    size'  = S.pack $ show $ S.length message'

