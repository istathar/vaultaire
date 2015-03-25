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
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
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
import Text.Printf
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.Origin
import Vaultaire.RollOver
import Vaultaire.Types
import Vaultaire.Util (fatal)

type EpochMap = HashMap Epoch
type BucketMap = HashMap Bucket

-- | State used by the writer when processing a batch of points.
data BatchState = BatchState
    { simple         :: !(EpochMap (BucketMap Builder))
    , extended       :: !(EpochMap (BucketMap Builder))
    , pending        :: !(EpochMap (BucketMap (Word64, [Word64 -> Builder])))
    , latestSimple   :: !TimeStamp
    , latestExtended :: !TimeStamp
    , dayMaps        :: !(DayMap, DayMap) -- ^ Simple, extended
    , bucketSize     :: !Word64
    , start          :: !UTCTime
    }

-- | Start a writer daemon, runs until shutdown.
startWriter :: DaemonArgs -> BucketSize -> IO ()
startWriter args bucket_size = handleMessages args (processBatch bucket_size)

-- | Gets the relevant batch state for a given BucketSize and simple
--   and extended DayMaps. The 'start' time is the current time.
batchStateNow :: BucketSize
              -> (DayMap, DayMap) -- ^ (simple daymap, extended daymap)
              -> IO BatchState
batchStateNow bucket_size dms =
    BatchState mempty mempty mempty 0 0 dms bucket_size <$> getCurrentTime

-- | Writes a batch of points. Will only write to regular
--   (external) buckets.
processBatch :: BucketSize
             -> Message
             -> Daemon ()
processBatch bucket_size (Message reply origin payload)
  = profileTime  WriterRequestLatency origin $ do
    profileCount WriterRequest        origin

    let bytes = S.length payload

    t1 <- liftIO getCurrentTime
    liftIO $ infoM "Writer.processBatch"
                (show origin ++ " Processing " ++ printf "%9d" bytes ++ " B")

    write_state <- withLockShared (originLockOID origin) $ do
        refreshOriginDays origin
        simple_dm <- withSimpleDayMap origin id
        extended_dm  <- withExtendedDayMap origin id
        case (,) <$> simple_dm <*> extended_dm of
            Nothing -> return Nothing
            Just dms -> do
                -- Most messages simply need to be placed into the correct epoch
                -- and bucket, extended ones are a little more complex in that they
                -- have to be stored as an offset to a pending write to the
                -- extended buckets.

                s <- liftIO $ batchStateNow bucket_size dms
                let ((sp, ep), s') = flip runState s
                                   $ processPoints 0 payload (dayMaps s) origin
                                                   (latestSimple s) (latestExtended s)

                profileCountN WriterSimplePoints  origin sp
                profileCountN WriterExtendedPoints origin ep

                return $ Just s'

    result <- case write_state of
        Nothing -> reply InvalidWriteOrigin
        Just s -> do
            wt1 <- liftIO getCurrentTime
            profileTime  WriterCephLatency origin
                       $ write External origin True s
            wt2 <- liftIO getCurrentTime
            liftIO . debugM "Writer.processBatch" $ concat [
                "Wrote simple objects at ",
                fmtWriteRate bytes wt2 wt1,
                " kB/s"]
            reply OnDisk

    t2 <- liftIO getCurrentTime
    let delta_padded = fmtWriteRate bytes t2 t1
    liftIO $ infoM "Writer.processBatch"
                (show origin ++ " Finished   " ++ delta_padded ++ " kB/s")
    return result
  where
    fmtWriteRate :: Int -> UTCTime -> UTCTime -> String
    fmtWriteRate bytes end start = printf "%9.1f" . writeRate bytes $ diffUTCTime end start

    writeRate :: Int -> NominalDiffTime -> Float
    writeRate bytes d = ((fromRational . toRational) bytes / (fromRational . toRational) d) / 1000

-- | Given a message consisting of one or more simple or extended
--   points, write them to the vault.
processPoints :: MonadState BatchState m
              => Word64            -- ^ Offset
              -> ByteString        -- ^ Raw message
              -> (DayMap, DayMap)  -- ^ (simple daymap, extended daymap)
              -> Origin            -- ^ Origin to write to
              -> TimeStamp         -- ^ Latest simple timestamp
              -> TimeStamp         -- ^ Latest extended timestamp
              -> m (Int, Int)      -- ^ Number of (simple, extended) points processed
processPoints offset message day_maps origin latest_simple latest_ext
    | fromIntegral offset >= S.length message = do
        modify (\s -> s { latestSimple   = latest_simple
                        , latestExtended = latest_ext })
        return (0,0)
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
                (s,e) <- processPoints (offset + 24 + len) message day_maps origin latest_simple t
                return (s,e+1)

            else do
                let message_bytes = runUnpacking (getBytesAt offset 24) message
                let simple_bucket = calculateBucketNumber simple_buckets address
                appendSimple simple_epoch simple_bucket message_bytes
                let !t | time > latest_simple = time
                       | otherwise            = latest_simple
                (s,e) <- processPoints (offset + 24) message day_maps origin t latest_ext
                return (s+1,e)

-- | Unpacks a message starting from the given offset. If it corresponds
--   to a simple point, the 'Payload' will be the value; if extended,
--   the 'Payload' will be the number of bytes in the value.
parseMessageAt :: Word64 -> Unpacking (Address, TimeStamp, Payload)
parseMessageAt offset = do
    unpackSetPosition (fromIntegral offset)
    (,,) <$> (Address <$> getWord64LE) <*> (TimeStamp <$> getWord64LE) <*> getWord64LE

-- | Gets the specified number of bytes, starting from the specified
--   offset.
getBytesAt :: Word64 -- ^ Offset
           -> Word64 -- ^ Number of bytes
           -> Unpacking ByteString
getBytesAt offset len = do
    unpackSetPosition (fromIntegral offset)
    getBytes (fromIntegral len)

-- | This one is pretty simple, simply append to the builder within the bucket
--   map, which is within an epoch map itself. Yes, this is two map lookups per
--   insert.
appendSimple :: MonadState BatchState m
             => Epoch -> Bucket -> ByteString -> m ()
appendSimple epoch bucket bytes = do
    s <- get
    let builder = byteString bytes
    let simple_map = HashMap.lookupDefault HashMap.empty epoch (simple s)
    let simple_map' = HashMap.insertWith (flip (<>)) bucket builder simple_map
    let !simple' = HashMap.insert epoch simple_map' (simple s)
    put $ s { simple = simple' }

-- | Analogous to 'appendSimple' for extended points.
appendExtended :: MonadState BatchState m
               => Epoch -> Bucket -> Address -> TimeStamp -> Word64 -> ByteString -> m ()
appendExtended epoch bucket (Address address) (TimeStamp time) len string = do
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
--
--   This function is used to write both internal and external buckets;
--   this is controlled by the first parameter.
write :: Namespace
      -> Origin
      -> Bool
      -> BatchState
      -> Daemon ()
write ns origin do_rollovers s = do
    let namespaced_origin = namespaceOrigin ns origin
    extended_offsets <- writeExtendedBuckets namespaced_origin

    let simple_buckets = applyOffsets extended_offsets (simple s) (pending s)
    simple_offsets <- stepTwo simple_buckets namespaced_origin

    -- Update latest files after the writes have gone down to disk, in case
    -- something happens between now and sending all the acks.
    when do_rollovers $ do
        updateSimpleLatest namespaced_origin (latestSimple s)
        updateExtendedLatest namespaced_origin (latestExtended s)

    -- 4. Do any rollovers
    when do_rollovers $ do
        let limit = bucketSize s
        -- We want to ensure that the rollover only happens if an offset is
        -- exceeded for the latest epoch, otherwise we get duplicate
        -- rollovers.
        (simple_epoch, n_simple_buckets) <- getLatestEpoch withSimpleDayMap namespaced_origin
        (extended_epoch, n_extended_buckets) <- getLatestEpoch withExtendedDayMap namespaced_origin

        when (offsetExceeded simple_offsets simple_epoch limit)
                (rollOverSimpleDay namespaced_origin n_simple_buckets)
        when (offsetExceeded extended_offsets extended_epoch limit)
                 (rollOverExtendedDay namespaced_origin n_extended_buckets)
  where
    getLatestEpoch f o =
        f o (lookupFirst maxBound) >>= mustBucket

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
    writeExtendedBuckets o =
        withLockExclusive (writeLockOID o) $ liftPool $ do
            -- First pass to get current offsets
            offsets <- forWithKey (extended s) $ \epoch buckets -> do

                -- Make requests for the entire epoch
                liftIO $ debugM "Writer.writeExtendedBuckets" "Stat extended objects"
                stats <- forWithKey buckets $ \bucket _ ->
                    extendedOffset o epoch bucket

                -- Then extract the fileSize from those requests
                for stats $ \async_stat -> do
                    result <- look async_stat
                    case result of
                        Left (NoEntity{..}) ->
                            return 0
                        Left e ->
                            fatal "Writer.writeExtendedBuckets" $
                                "extended bucket stat: " ++ show e
                        Right st ->
                            return $ fileSize st

            -- Second pass to write the extended data
            _ <- forWithKey (extended s) $ \epoch buckets -> do
                liftIO $ debugM "Writer.writeExtendedBuckets" "Write extended objects"
                writes <- forWithKey buckets $ \bucket builder -> do
                    let payload = toStrict $ toLazyByteString builder
                    writeExtended o epoch bucket payload

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
    stepTwo simple_buckets o = liftPool $
        forWithKey simple_buckets $ \epoch buckets -> do
            liftIO $ debugM "Writer.stepTwo" "Write simple objects"
            writes <- forWithKey buckets $ \bucket builder -> do
                let payload = toStrict $ toLazyByteString builder
                writeSimple o epoch bucket payload
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

-- | Get the file size and mtime of an extended bucket.
extendedOffset :: Origin -> Epoch -> Bucket -> Pool (AsyncRead StatResult)
extendedOffset o e b =
    runAsync $ runObject (bucketOID o e b "extended") stat

-- | Writes an extended point to a bucket.
writeExtended :: Origin -> Epoch -> Bucket -> ByteString -> Pool AsyncWrite
writeExtended o e b payload =
    runAsync $ runObject (bucketOID o e b "extended") (append payload)

-- | Writes a simple point to a bucket.
writeSimple :: Origin -> Epoch -> Bucket -> ByteString -> Pool (AsyncRead StatResult, AsyncWrite)
writeSimple o e b payload =
    runAsync $ runObject (bucketOID o e b "simple") $
        (,) <$> stat <*> append payload

-- | Object ID of the write lock object for an origin.
writeLockOID :: Origin -> ByteString
writeLockOID (Origin o') =
    "02_" <> o' <> "_write_lock"
