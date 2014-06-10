{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module Vaultaire.Reader
(
    startReader,
    readExtended,
    ReadDetails(..),
    getBuckets,
    -- Testing
    classifyPayload,
    SomeRequest(..),
    Request(..),
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Cont
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.Packer
import Pipes
import System.Log.Logger
import System.Rados.Monadic
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.ReaderAlgorithms (mergeSimpleExtended, processBucket)
import Vaultaire.Types

data Simple
data Extended
data Invalid

data ReadDetails = ReadDetails Address Time Time

data Request typ where
    Simple   :: ReadDetails -> Request Simple
    Extended :: ReadDetails -> Request Extended
    Invalid  :: String -> Request Invalid

data SomeRequest where
    SomeRequest :: Request typ -> SomeRequest

-- | Start a writer daemon, never returns.
startReader :: String           -- ^ Broker
            -> Maybe ByteString -- ^ Username for Ceph
            -> ByteString       -- ^ Pool name for Ceph
            -> IO ()
startReader broker user pool = runDaemon broker user pool $
    forever $ nextMessage >>= handleRequest

handleRequest :: Message -> Daemon ()
handleRequest (Message reply_f origin' payload') = do
    () <- case classifyPayload payload' of
        SomeRequest r@Simple{}   -> processSimple r origin' reply_f
        SomeRequest r@Extended{} -> processExtended r origin' reply_f
        SomeRequest r@Invalid{}  -> processInvalid r
    reply_f EndOfStream

yieldNotNull :: Monad m => ByteString -> Pipe i ByteString m ()
yieldNotNull bs = unless (S.null bs) (yield bs)

processSimple :: Request Simple -> Origin -> ReplyF -> Daemon ()
processSimple s@(Simple (ReadDetails _ start end)) origin' reply_f = do
    refreshOriginDays origin'
    maybe_range <- withSimpleDayMap origin' (lookupRange start end)

    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readSimple origin' s)
                            (lift . reply_f . SimpleBurst)
        Nothing -> reply_f InvalidReadOrigin

readSimple :: Origin -> Request Simple
           -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readSimple origin' (Simple (ReadDetails addr start end)) = forever $ do
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

processExtended :: Request Extended -> Origin -> ReplyF -> Daemon ()
processExtended e@(Extended (ReadDetails _ start end)) origin' reply_f = do
    refreshOriginDays origin'
    maybe_range <- withExtendedDayMap origin' (lookupRange start end)
    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readExtended origin' e)
                            (lift . reply_f . ExtendedBurst)
        Nothing -> reply_f InvalidReadOrigin

readExtended :: Origin -> Request Extended
             -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readExtended origin (Extended (ReadDetails addr start end)) = forever $ do
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

processInvalid :: Request Invalid -> Daemon ()
processInvalid (Invalid err) =
    liftIO $ putStrLn $ "Failed to parse read request: " ++ err

classifyPayload :: ByteString -> SomeRequest
classifyPayload payload' =
    case tryUnpacking unpackReadDetails payload' of
        Left e -> SomeRequest $ Invalid $ show e
        Right d@(ReadDetails address _ _) ->
            if isAddressExtended address
                then SomeRequest $ Extended d
                else SomeRequest $ Simple d

unpackReadDetails :: Unpacking ReadDetails
unpackReadDetails = do
    request_type <- getWord64LE
    if request_type == 0
        then ReadDetails <$> (Address <$> getWord64LE) <*> getWord64LE <*> getWord64LE
        else fail "Zero header is reserved for future use"
