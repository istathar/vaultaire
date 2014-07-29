{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Vaultaire.Writer
(
    startWriter,
    -- Testing
    processPoints,
    appendExtended,
    appendSimple,
    write,
    batchStateNow,
    BatchState(..),
    Event(..),
) where

import Control.Applicative
import Control.Concurrent (MVar)
import Control.Monad
import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Monoid
import Data.Packer
import Data.Time
import Data.Traversable (for)
import Data.Word (Word64)
import System.Log.Logger
import System.Rados.Monadic hiding (async)
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.RollOver
import Vaultaire.Types
import Vaultaire.Util (fatal)

type EpochMap = HashMap Epoch
type BucketMap = HashMap Bucket

data BatchState = BatchState
    { simple         :: !(EpochMap (BucketMap Builder))
    , extended       :: !(EpochMap (BucketMap Builder))
    , pending        :: !(EpochMap (BucketMap (Word64, [Word64 -> Builder])))
    , latestSimple   :: !Time
    , latestExtended :: !Time
    , dayMaps        :: !(DayMap, DayMap) -- Simple, extended
    , bucketSize     :: !Word64
    , start          :: !UTCTime
    }

data Event = Msg Message | Tick

-- | Start a writer daemon, never returns.
startWriter :: String           -- ^ Broker
            -> Maybe ByteString -- ^ Username for Ceph
            -> ByteString       -- ^ Pool name for Ceph
            -> Word64           -- ^ Maximum bytes in bucket before rollover
            -> MVar ()          -- ^ Shutdown signal
            -> IO ()
startWriter broker user pool bucket_size shutdown = do
    handleMessages broker user pool shutdown (processBatch bucket_size)

batchStateNow :: Word64 -> (DayMap, DayMap) -> IO BatchState
batchStateNow bucket_size dms =
    BatchState mempty mempty mempty 0 0 dms bucket_size <$> getCurrentTime

processBatch :: Word64 -> Message -> Daemon ()
processBatch bucket_size (Message reply origin payload) = do
    write_state <- withLock (originLockOID origin) $ do
        refreshOriginDays origin
        simple_dm <- withSimpleDayMap origin id
        extended_dm  <- withExtendedDayMap origin id
        case (,) <$> simple_dm <*> extended_dm of
            Nothing -> return Nothing
            Just dms -> do
                liftIO $ debugM "Writer.processEvents" $
                                "Processing payload (" ++ show (BS.length payload)
                                ++ " bytes)"

                -- Most messages simply need to be placed into the correct epoch
                -- and bucket, extended ones are a little more complex in that they
                -- have to be stored as an offset to a pending write to the
                -- extended buckets.

                s <- liftIO $ batchStateNow bucket_size dms

                return . Just . flip execState s $
                    processPoints 0 payload (dayMaps s) origin (latestSimple s) (latestExtended s)
    case write_state of
        Nothing -> reply InvalidWriteOrigin
        Just s -> do
            write origin True s
            reply OnDisk

processPoints :: MonadState BatchState m
              => Word64 -> ByteString -> (DayMap, DayMap) -> Origin -> Time -> Time -> m ()
processPoints offset message day_maps origin latest_simple latest_ext
    | fromIntegral offset >= BS.length message = modify (\s -> s{ latestSimple = latest_simple
                                                                , latestExtended = latest_ext })
    | otherwise = do
        let (address, time, payload) = runUnpacking (parseMessageAt offset) message
        let (simple_epoch, simple_buckets) = lookupFirst time (fst day_maps)

        -- The LSB of the address lets us know if it is an extended message or
        -- not. Set means extended.
        if isAddressExtended address
            then do
                let len = fromIntegral payload
                let str | len == 0 = "" -- will fail bounds check without this
                        | otherwise =
                            runUnpacking (getBytesAt (offset + 24) len) message
                let (ext_epoch, ext_buckets) = lookupFirst time (snd day_maps)
                let ext_bucket = calculateBucketNumber ext_buckets address
                appendExtended ext_epoch ext_bucket address time len str
                let !t | time > latest_ext = time
                       | otherwise         = latest_ext
                processPoints (offset + 24 + len) message day_maps origin latest_simple t
            else do
                let message_bytes = runUnpacking (getBytesAt offset 24) message
                let simple_bucket = calculateBucketNumber simple_buckets address
                appendSimple simple_epoch simple_bucket message_bytes
                let !t | time > latest_simple = time
                       | otherwise            = latest_simple
                processPoints (offset + 24) message day_maps origin t latest_ext

parseMessageAt :: Word64 -> Unpacking (Address, Time, Payload)
parseMessageAt offset = do
    unpackSetPosition (fromIntegral offset)
    (,,) <$> (Address <$> getWord64LE) <*> getWord64LE <*> getWord64LE


getBytesAt :: Word64 -> Word64 -> Unpacking ByteString
getBytesAt offset len = do
    unpackSetPosition (fromIntegral offset)
    getBytes (fromIntegral len)

-- | This one is pretty simple, simply append to the builder within the bucket
-- map, which is within an epoch map itself. Yes, this is two map lookups per
-- insert.
appendSimple :: MonadState BatchState m
             => Epoch -> Bucket -> ByteString -> m ()
appendSimple epoch bucket bytes = do
    s <- get
    let builder = byteString bytes
    let simple_map = HashMap.lookupDefault HashMap.empty epoch (simple s)
    let simple_map' = HashMap.insertWith (flip (<>)) bucket builder simple_map
    let !simple' = HashMap.insert epoch simple_map' (simple s)
    put $ s { simple = simple' }

appendExtended :: MonadState BatchState m
               => Epoch -> Bucket -> Address -> Time -> Word64 -> ByteString -> m ()
appendExtended epoch bucket (Address address) time len string = do
    s <- get

    -- First we write to the simple bucket, inserting a closure that will
    -- return a builder given an offset of the extended bucket write.
    let pending_map = HashMap.lookupDefault HashMap.empty epoch (pending s)

    -- Starting from zero, we write to the current offset and point the next
    -- extended point to the end of that write.
    let (os, fs) = HashMap.lookupDefault (0, []) bucket pending_map
    let os' = os + len + 8

    -- Create the closure for the pointer to the extended bucket
    let prefix = word64LE address <> word64LE time
    let fs' = (\base_offset -> prefix <> word64LE (base_offset + os)):fs

    -- Update the bucket,
    let pending_map' = HashMap.insert bucket (os', fs') pending_map
    let pending' = HashMap.insert epoch pending_map' (pending s)

    -- Now the data goes into the extended bucket.
    let builder = word64LE len <> byteString string
    let ext_map= HashMap.lookupDefault HashMap.empty epoch (extended s)
    let ext_map' = HashMap.insertWith (flip (<>)) bucket builder ext_map
    let extended' = HashMap.insert epoch ext_map' (extended s)

    put $ s { pending = pending', extended = extended' }

-- | Write happens in three stages:
--   1. Extended buckets are written to disk and the offset is noted.
--   2. Simple buckets are written to disk with the pending writes applied.
--   3. Acks are sent
--   4. Any rollovers are done
write :: Origin -> Bool -> BatchState -> Daemon ()
write origin do_rollovers s = do
    extended_offsets <- writeExtendedBuckets

    let simple_buckets = applyOffsets extended_offsets (simple s) (pending s)
    simple_offsets <- stepTwo simple_buckets

    -- Update latest files after the writes have gone down to disk, in case
    -- something happens between now and sending all the acks.
    when do_rollovers $ do
        updateSimpleLatest origin (latestSimple s)
        updateExtendedLatest origin (latestExtended s)

    -- 4. Do any rollovers
    when do_rollovers $ do
        let limit = bucketSize s
        -- We want to ensure that the rollover only happens if an offset is
        -- exceeded for the latest epoch, otherwise we get duplicate
        -- rollovers.
        (simple_epoch, n_simple_buckets) <- getLatestEpoch withSimpleDayMap
        (extended_epoch, n_extended_buckets) <- getLatestEpoch withExtendedDayMap

        when (offsetExceeded simple_offsets simple_epoch limit)
                (rollOverSimpleDay origin n_simple_buckets)
        when (offsetExceeded extended_offsets extended_epoch limit)
                 (rollOverExtendedDay origin n_extended_buckets)
  where
    getLatestEpoch f =
        f origin (lookupFirst maxBound) >>= mustBucket

    mustBucket = maybe (error "could not find n_buckets for roll over") return

    offsetExceeded offsetss epoch limit =
        case HashMap.lookup epoch offsetss of
            Nothing ->
                -- The latest epoch wasn't written to this time, so we don't
                -- need to rollover.
                False
            Just offsets ->
                HashMap.foldr max 0 offsets > limit

    -- 1. Write extended buckets. We lock the entire origin for write as we
    -- will be operating on most buckets most of the time.
    writeExtendedBuckets =
        withExLock (writeLockOID origin) $ liftPool $ do
            -- First pass to get current offsets
            offsets <- forWithKey (extended s) $ \epoch buckets -> do

                -- Make requests for the entire epoch
                stats <- forWithKey buckets $ \bucket _ ->
                    extendedOffset origin epoch bucket

                -- Then extract the fileSize from those requests
                for stats $ \async_stat -> do
                    result <- look async_stat
                    case result of
                        Left (NoEntity{..}) ->
                            return 0
                        Left e ->
                            fatal "Writer.writeExtendedBuckets" $
                                "extended bucket read: " ++ show e
                        Right st ->
                            return $ fileSize st

            -- Second pass to write the extended data
            _ <- forWithKey (extended s) $ \epoch buckets -> do
                writes <- forWithKey buckets $ \bucket builder -> do
                    let payload = toStrict $ toLazyByteString builder
                    writeExtended origin epoch bucket payload

                for writes $ \async_write -> do
                    result <- waitSafe async_write
                    case result of
                        Just e -> fatal "Writer.writeExtendedBuckets" $
                            "extended bucket write: " ++ show e
                        Nothing -> return ()

            return offsets

    -- Given two maps, one of offsets and one of closures, we walk through
    -- applying one to the other. We then append that to the map of simple
    -- writes in order to achieve one write.
    applyOffsets offset_map =
        HashMap.foldlWithKey' applyEpochs
      where
        applyEpochs simple_map' epoch =
            HashMap.foldlWithKey' (applyBuckets epoch) simple_map'

        applyBuckets epoch simple_map'' bucket (_, fs) =
            let offset = HashMap.lookup epoch offset_map >>= HashMap.lookup bucket
                simple_buckets = HashMap.lookupDefault HashMap.empty epoch simple_map''
            in case offset of
                Nothing -> fatal "Writer.applyOffsets"
                                 "No offset for extended point!"
                Just os ->
                    let builder = mconcat $ reverse $ map ($os) fs
                        simple_buckets' = HashMap.insertWith (<>) bucket builder simple_buckets
                    in HashMap.insert epoch simple_buckets' simple_map''

    -- Final write,
    stepTwo simple_buckets = liftPool $
        forWithKey simple_buckets $ \epoch buckets -> do
            writes <- forWithKey buckets $ \bucket builder -> do
                let payload = toStrict $ toLazyByteString builder
                writeSimple origin epoch bucket payload
            for writes $ \(async_stat, async_write) -> do
                w <- waitSafe async_write
                case w of
                    Just e -> fatal "Writer.stepTwo" $
                                    "simple bucket write: " ++ show e
                    Nothing -> do
                        r <- look async_stat
                        case r of
                            Left NoEntity{} -> return 0
                            Left e   -> fatal "Writer.stepTwo" $
                                              "simple bucket read: " ++ show e
                            Right st -> return $ fileSize st

    forWithKey = flip HashMap.traverseWithKey

extendedOffset :: Origin -> Epoch -> Bucket -> Pool (AsyncRead StatResult)
extendedOffset o e b =
    runAsync $ runObject (bucketOID o e b "extended") stat

writeExtended :: Origin -> Epoch -> Bucket -> ByteString -> Pool AsyncWrite
writeExtended o e b payload =
    runAsync $ runObject (bucketOID o e b "extended") (append payload)

writeSimple :: Origin -> Epoch -> Bucket -> ByteString -> Pool (AsyncRead StatResult, AsyncWrite)
writeSimple o e b payload =
    runAsync $ runObject (bucketOID o e b "simple") $
        (,) <$> stat <*> append payload

writeLockOID :: Origin -> ByteString
writeLockOID (Origin o') =
    "02_" `BS.append` o' `BS.append` "_write_lock"
