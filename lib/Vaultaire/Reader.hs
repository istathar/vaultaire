{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Reader
(
    startReader,
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
import Data.Packer
import Pipes
import System.Rados.Monadic
import Vaultaire.CoreTypes
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.OriginMap
import Vaultaire.ReaderAlgorithms (mergeSimpleExtended, processBucket)


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
        SomeRequest r@Invalid{}  -> processInvalid r reply_f
    reply_f Success

processSimple :: Request Simple -> Origin -> ReplyF -> Daemon ()
processSimple s@(Simple (ReadDetails _ start end)) origin' reply_f = do
    refreshOriginDays origin'
    maybe_range <- withSimpleDayMap origin' (lookupRange start end)

    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readSimple origin' s
                                                       (reply_f . Failure))
                            (lift . reply_f . Response)
        Nothing -> reply_f $ Failure "Invalid origin"

readSimple :: Origin -> Request Simple -> (ByteString -> Daemon ())
           -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readSimple origin' (Simple (ReadDetails addr start end)) fail_f = forever $ do
    (epoch, num_buckets) <- await
    let bucket = calculateBucketNumber num_buckets addr
    let bucket_oid = bucketOID origin' epoch bucket "simple"
    contents <- lift $ liftPool $ runObject bucket_oid readFull
    case contents of
        Left (NoEntity{}) -> return ()
        Left e -> do
            liftIO $ putStrLn $ "Ceph error getting simple bucket: " ++ show e
            lift $ fail_f "Failed to retrieve bucket"
        Right unprocessed ->
            yield $ runST $ processBucket unprocessed addr start end

processExtended :: Request Extended -> Origin -> ReplyF -> Daemon ()
processExtended e@(Extended (ReadDetails _ start end)) origin' reply_f = do
    refreshOriginDays origin'
    maybe_range <- withExtendedDayMap origin' (lookupRange start end)
    case maybe_range of
        Just range ->
            runEffect $ for (each range >-> readExtended origin' e
                                                         (reply_f . Failure))
                            (lift . reply_f . Response)
        Nothing -> reply_f $ Failure "Invalid origin"

readExtended :: Origin -> Request Extended -> (ByteString -> Daemon ())
             -> Pipe (Epoch, NumBuckets) ByteString Daemon ()
readExtended origin (Extended (ReadDetails addr start end)) fail_f = forever $ do
    (epoch, num_buckets) <- await
    let bucket = calculateBucketNumber num_buckets addr
    let simple_oid = bucketOID origin epoch bucket "simple"
    let extended_oid = bucketOID origin epoch bucket "extended"

    (a_simple, a_extended) <- lift $ liftPool $ runAsync $
        (,) <$> runObject simple_oid readFull
            <*> runObject extended_oid readFull

    buckets <- lift $ getBuckets a_simple a_extended

    case buckets of
        Nothing -> return ()
        Just (s,e) -> yield $ runST $ mergeSimpleExtended s e addr start end
  where
    getBuckets a_simple a_extended = do
        maybe_simple <- look a_simple >>= (\c -> case c of
            Left (NoEntity{}) -> return Nothing
            Left e -> do
                liftIO $ putStrLn $ "Ceph error getting simple bucket: " ++ show e
                fail_f "Failed to retrieve bucket"
                return Nothing
            Right unprocessed -> return $ Just unprocessed)

        maybe_extended <- look a_extended >>= (\c -> case c of
            Left (NoEntity{}) -> return Nothing
            Left e -> do
                liftIO $ putStrLn $ "Ceph error getting extended bucket: " ++ show e
                fail_f "Failed to retrieve bucket"
                return Nothing
            Right unprocessed -> return $ Just unprocessed)

        return $ (,) <$> maybe_simple <*> maybe_extended

processInvalid :: Request Invalid -> ReplyF -> Daemon ()
processInvalid (Invalid err) reply_f = do
    liftIO $ putStrLn $ "Failed to parse read request: " ++ err
    reply_f $ Failure "Invalid request"

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
