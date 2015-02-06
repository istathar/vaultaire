{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module Vaultaire.Reader
(
    startReader,
    readExtended,
    readExtendedInternal,
    getBuckets,
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Cont
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Pipes
import System.Log.Logger
import System.Rados.Monadic

import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.Origin
import Vaultaire.ReaderAlgorithms (mergeSimpleExtended, processBucket)
import Vaultaire.Types

-- | Start a writer daemon, never returns.
startReader :: DaemonArgs -> IO ()
startReader = flip handleMessages handleRequest

handleRequest :: Message -> Daemon ()
handleRequest (Message reply_f origin payload)
  = profileTime  ReaderRequestLatency origin $ do
    profileCount ReaderRequest origin

    case fromWire payload of
        Right req -> do
            liftIO $ infoM "Reader.handleRequest" (show origin ++ " Read " ++ show req)
            case req of
                SimpleReadRequest addr start end ->
                    processSimple addr start end origin reply_f
                ExtendedReadRequest addr start end ->
                    processExtended addr start end origin reply_f
            reply_f EndOfStream
        Left e ->
            liftIO . errorM "Reader.handleRequest" $
                            "failed to decode request: " ++ show e

yieldNotNull :: Monad m => ByteString -> Pipe i ByteString m ()
yieldNotNull bs = unless (S.null bs) (yield bs)

processSimple :: Address -> TimeStamp -> TimeStamp -> Origin -> ReplyF -> Daemon ()
processSimple addr start end origin reply_f = do
    profileCount ReaderSimplePoints origin

    refreshOriginDays origin
    maybe_range <- withSimpleDayMap origin (lookupRange start end)

    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readSimple origin addr start end)
                            (lift . reply_f . SimpleStream . SimpleBurst)
        Nothing -> reply_f InvalidReadOrigin

-- | readSimple reads SimplePoints in a given time range from the vault.
--   Cannot be used to read from internal (contents) buckets; regular
--   time series only.
readSimple :: Origin -> Address -> TimeStamp -> TimeStamp
           -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readSimple origin addr start end = forever $ do
    (epoch, num_buckets) <- await
    let bucket = calculateBucketNumber num_buckets addr
    let bucket_oid = bucketOID origin epoch bucket "simple"
    contents <- lift     $ profileTime ReaderCephLatency origin
              $ liftPool $ runObject bucket_oid readFull
    case contents of
        Left (NoEntity{}) -> return ()
        Left e ->
            liftIO $ errorM "Reader.readSimple" $
                            "Ceph error getting simple bucket: " ++ show e
        Right unprocessed -> do
            let bs = runST $ processBucket unprocessed addr start end
            -- This division should have no remainer, as the bytestring should
            -- contain whole simple points. If not, it's garbage.
            lift $ profileCountN ReaderSimplePoints origin (S.length bs `div` 24)
            yieldNotNull bs

processExtended :: Address -> TimeStamp -> TimeStamp -> Origin -> ReplyF -> Daemon ()
processExtended addr start end origin reply_f = do
    refreshOriginDays origin
    maybe_range <- withExtendedDayMap origin (lookupRange start end)
    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readExtended origin addr start end)
                            (lift . reply_f . ExtendedStream . ExtendedBurst)
        Nothing -> reply_f InvalidReadOrigin

-- | readExtended' reads extended points from either an internal or an
--   external bucket, depending on the value of the first parameter.
readExtended' :: Namespace
              -> Origin
              -> Address
              -> TimeStamp
              -> TimeStamp
              -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readExtended' ns origin addr start end = forever $ do
    (epoch, num_buckets) <- await
    let bucket = calculateBucketNumber num_buckets addr
    buckets <- lift $ getBuckets ns origin epoch bucket
    case buckets of
        Nothing -> return ()
        Just (s,e) -> do
            let bs = runST $ mergeSimpleExtended s e addr start end
            lift $ profileCountN ReaderExtendedPoints origin (S.length bs `div` 24)
            yieldNotNull bs

-- | readExtended reads ExtendedPoints in a given time range. Cannot be
--   used to read internal buckets.
readExtended :: Origin -> Address -> TimeStamp -> TimeStamp
             -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readExtended = readExtended' External

-- | readExtendedInternal reads internal ExtendedPoints in a given time range.
--   Cannot be used to read regular buckets.
readExtendedInternal :: Origin -> Address -> TimeStamp -> TimeStamp
                     -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readExtendedInternal = readExtended' Internal

-- | Retrieve simple and extended buckets in parallel. Can be used for
--   either regular or internal buckets (controlled by the 'internal'
--   flag).
getBuckets :: Namespace
           -> Origin
           -> Epoch
           -> Bucket
           -> Daemon (Maybe (ByteString, ByteString))
getBuckets ns origin epoch bucket = do
    let namespaced_origin = namespaceOrigin ns origin
    let simple_oid = bucketOID namespaced_origin epoch bucket "simple"
    let extended_oid = bucketOID namespaced_origin epoch bucket "extended"

    -- Request both async
    (a_simple, a_extended) <- profileTime ReaderCephLatency origin
                            $ liftPool $ runAsync
                            $ (,) <$> runObject simple_oid readFull
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
