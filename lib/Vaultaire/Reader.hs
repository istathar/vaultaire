{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module Vaultaire.Reader
(
    startReader,
    readExtended,
    getBuckets,
) where

import Control.Applicative
import Control.Concurrent (MVar)
import Control.Monad
import Control.Monad.Cont
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Pipes
import System.Log.Logger
import System.Rados.Monadic
import Text.Printf

import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.ReaderAlgorithms (mergeSimpleExtended, processBucket)
import Vaultaire.Types

-- | Start a writer daemon, never returns.
startReader :: String           -- ^ Broker
            -> Maybe ByteString -- ^ Username for Ceph
            -> ByteString       -- ^ Pool name for Ceph
            -> MVar ()
            -> IO ()
startReader broker user pool shutdown = do
    handleMessages broker user pool shutdown handleRequest

handleRequest :: Message -> Daemon ()
handleRequest (Message reply_f origin' payload') =
    case fromWire payload' of
        Right req -> do
            liftIO $ debugM "Reader.handleRequest" (display req)
            case req of
                SimpleReadRequest addr start end ->
                    processSimple addr start end origin' reply_f
                ExtendedReadRequest addr start end ->
                    processExtended addr start end origin' reply_f
            reply_f EndOfStream
        Left e ->
            liftIO . errorM "Reader.handleRequest" $
                            "failed to decode request: " ++ show e
  where
    display (SimpleReadRequest addr start end)   = "Read " ++ show origin' ++ " " ++ show addr ++ " (s) " ++ format start ++ " to " ++ format end
    display (ExtendedReadRequest addr start end) = "Read " ++ show origin' ++ " " ++ show addr ++ " (e) " ++ format start ++ " to " ++ format end
    format (TimeStamp t) = printf "%010d" (t `div` 1000000000)

yieldNotNull :: Monad m => ByteString -> Pipe i ByteString m ()
yieldNotNull bs = unless (S.null bs) (yield bs)

processSimple :: Address -> TimeStamp -> TimeStamp -> Origin -> ReplyF -> Daemon ()
processSimple addr start end origin' reply_f = do
    refreshOriginDays origin'
    maybe_range <- withSimpleDayMap origin' (lookupRange start end)

    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readSimple origin' addr start end)
                            (lift . reply_f . SimpleStream . SimpleBurst)
        Nothing -> reply_f InvalidReadOrigin

readSimple :: Origin -> Address -> TimeStamp -> TimeStamp
           -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readSimple origin' addr start end = forever $ do
    (epoch, num_buckets) <- await
    let bucket = calculateBucketNumber num_buckets addr
    let bucket_oid = bucketOID origin' epoch bucket "simple"
    contents <- lift $ liftPool $ runObject bucket_oid readFull
    case contents of
        Left (NoEntity{}) -> return ()
        Left e ->
            liftIO $ errorM "Reader.readSimple" $
                            "Ceph error getting simple bucket: " ++ show e
        Right unprocessed -> yieldNotNull $ runST $
            processBucket unprocessed addr start end

processExtended :: Address -> TimeStamp -> TimeStamp -> Origin -> ReplyF -> Daemon ()
processExtended addr start end origin' reply_f = do
    refreshOriginDays origin'
    maybe_range <- withExtendedDayMap origin' (lookupRange start end)
    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readExtended origin' addr start end)
                            (lift . reply_f . ExtendedStream . ExtendedBurst)
        Nothing -> reply_f InvalidReadOrigin

readExtended :: Origin -> Address -> TimeStamp -> TimeStamp
             -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readExtended origin addr start end = forever $ do
    (epoch, num_buckets) <- await
    let bucket = calculateBucketNumber num_buckets addr
    buckets <- lift $ getBuckets origin epoch bucket
    case buckets of
        Nothing -> return ()
        Just (s,e) -> yieldNotNull $ runST $
            mergeSimpleExtended s e addr start end

-- | Retrieve simple and extended buckets in parallel
getBuckets :: Origin
           -> Epoch
           -> Bucket
           -> Daemon (Maybe (ByteString, ByteString))
getBuckets origin epoch bucket = do
    let simple_oid = bucketOID origin epoch bucket "simple"
    let extended_oid = bucketOID origin epoch bucket "extended"

    -- Request both async
    (a_simple, a_extended) <- liftPool $ runAsync $
        (,) <$> runObject simple_oid readFull
            <*> runObject extended_oid readFull

    -- Check both errors
    maybe_simple <- look a_simple >>= (\c -> case c of
        Left (NoEntity{}) -> return Nothing
        Left e -> do
            liftIO $ errorM "Reader.getBuckets" $
                            "Ceph error getting simple bucket: " ++ show e
            return Nothing
        Right unprocessed -> return $ Just unprocessed)

    maybe_extended <- look a_extended >>= (\c -> case c of
        Left (NoEntity{}) -> return Nothing
        Left e -> do
            liftIO $ errorM "Reader.getBuckets" $
                            "Ceph error getting extended bucket: " ++ show e
            return Nothing
        Right unprocessed -> return $ Just unprocessed)

    return $ (,) <$> maybe_simple <*> maybe_extended
