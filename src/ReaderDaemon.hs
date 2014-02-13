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

module ReaderDaemon
(

)
where

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
import Options.Applicative
import System.Environment (getArgs, getProgName)
import System.Rados
import System.ZMQ4.Monadic

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents


data Options = Options {
    argBrokerHost :: !String
}


findPoints = undefined
{-
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
-}

parseRequestMessage :: ByteString -> Either String [Request]
parseRequestMessage message' =
    decodeRequestMulti message'





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

    let cm = Map.empty

    runZMQ $ do
        req <- socket Req
        connect req ("tcp://" ++ broker ++ ":5571")

        tele <- socket Pub
        bind tele "tcp://*:5579"

        linkThread . forever $ do
            (k,v) <- liftIO $ readChan telemetry
            when d $ liftIO $ putStrLn $ printf "%-10s %-8s" (k ++ ":") v
            let reply = [S.pack k, S.pack v]
            sendMulti tele (fromList reply)


        forever $ do
            u' <- receive req
            t <- getCurrentTime

            (reply', o', cm1) <- case parseRequestMessage u' of
                Left err -> do
                    -- temporary, replace with telemetry
                    debug err

                    return $ (S.pack err, S.empty, cm)
                Right q -> do
                    -- temporary, replace with zmq message part?
                    let o' = qOrigin $ head q

                    ps <- findPoints cm o' q
                    let y' = encodePoints ps

                    -- temporary, replace with telemetry
                    debug $ S.length y'

                    return $ (y', o', cm)


            send req [] reply'

            liftIO $ debugTime t

program :: Options -> IO ()
program (Options broker) = do
    runZMQ $ do
        return ()


toplevel :: Parser Options
toplevel = Options
    <$> argument str
            (metavar "BROKER" <>
             help "Host name or IP address of broker to request from")


commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Process to handle requests for data points from the vault" <>
                header "A data vault for metrics")

