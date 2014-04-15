{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Vaultaire.Writer
(
    startWriter,
    -- Testing
    processMessage,
    processPoints,
    appendExtended,
    appendSimple,
    BatchState(..),
    batchStateNow
) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.State.Strict
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.ByteString.Lazy.Builder
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Maybe
import Data.Monoid
import Data.Packer
import Data.Time
import Data.Word (Word64)
import Pipes
import Pipes.Concurrent
import Pipes.Lift
import Text.Printf
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.OriginMap
import Vaultaire.RollOver
import Vaultaire.Util (linkThread)

data Stream = Packet Message | Tick

type Address = Word64
type Payload = Word64
type Bucket  = Word64

type FlyingThreadMonsterMap = OriginMap (Output Message)

type EpochMap = HashMap Epoch
type BucketMap = HashMap Bucket

data BatchState = BatchState
    { replyFs  :: [Response -> Daemon ()]
    , normal   :: EpochMap (BucketMap Builder)
    , extended :: EpochMap (BucketMap Builder)
    , pending  :: EpochMap (BucketMap (Word64, [Word64 -> Builder]))
    , dayMaps  :: (DayMap, DayMap) -- Simple, extended
    , start    :: UTCTime
    }

tickRate :: Int
tickRate = 1000000000 `div` 100

-- | Start a writer daemon, never returns.
startWriter :: String           -- ^ Broker
            -> Maybe ByteString -- ^ Username for Ceph
            -> ByteString       -- ^ Pool name for Ceph
            -> NominalDiffTime
            -> IO ()
startWriter broker user pool batch_period = runDaemon broker user pool $ do
    runEffect $ lift nextMessage
             >~ evalStateP emptyOriginMap (flyingThreadMonster batch_period)
    error "startWriter: impossible"

-- If the incoming message currently has a processing thread running for that
-- origin, feed the message into that thread. Otherwise create a new one and
-- feed it in.
flyingThreadMonster :: NominalDiffTime
                    -> Consumer Message (StateT FlyingThreadMonsterMap Daemon) ()
flyingThreadMonster batch_period = do
    m@(Message _ origin' _) <- await
    ftm <- get
    case originLookup origin' ftm of
        Just output -> do
            sent <- send' output m
            -- If it wasn't sent, the thread has shut itself down.
            if sent
                then startThread origin' ftm m
                else flyingThreadMonster batch_period
        Nothing   -> startThread origin' ftm m
  where
    send' o = liftIO . atomically . send o

    startThread origin' ftm m = do
        (output, seal, input) <- liftIO $ spawn' Single
        lift . lift $ async (processBatch batch_period origin' seal input)
        must_send <- send' output m
        unless must_send $ error "ftm died immediately, bailing"
        lift . put $ originInsert origin' output ftm
        flyingThreadMonster batch_period

batchStateNow :: (DayMap, DayMap) -> IO BatchState
batchStateNow dms = do
    BatchState mempty mempty mempty mempty dms <$> getCurrentTime

-- | The flying thread monster has done the hard work for us and sorted
-- incoming bursts by origin. Now we simply need to process these messages with
-- local state, writing to ceph when our collection time has elapsed.
processBatch :: NominalDiffTime -> Origin -> Input Message -> STM () -> Daemon ()
processBatch batch_period origin' input seal = do
    refreshOriginDays origin'
    simple_dm <- withSimpleDayMap origin' id
    extended_dm  <- withExtendedDayMap origin' id
    case (,) <$> simple_dm <*> extended_dm of
        Nothing ->
            -- Reply to first message with an error, try again on the next
            -- message. This is a potential DOS of sorts, however is needed
            -- unless we know when to expire
            runEffect $ fromInput input >-> badOrigin
        Just dms -> do
            -- Process for a batch period
            start_state <- liftIO $ batchStateNow dms
            runEffect $ fromInput input
                    >-> evalStateP start_state (processMessage batch_period)
                    >-> write
            liftIO $ atomically seal

badOrigin :: Consumer Message Daemon ()
badOrigin = do
    Message reply_f _ _ <- await
    lift $ reply_f $ Failure "No such origin"

feedTicks :: Producer Stream IO b
feedTicks = forever $ (lift $ threadDelay tickRate) >> yield Tick

-- | Place a message into state, wherever it belongs.
processMessage :: (MonadState BatchState m, MonadIO m)
               => NominalDiffTime -> Pipe Message BatchState m ()
processMessage batch_period = do
    -- Most messages simply need to be placed into the correct epoch and
    -- bucket, extended ones are a little more complex in that they have to be
    -- stored as an offset to a pending write to the extended buckets.
    Message rf origin' payload' <- await

    -- Append the replyf for this message
    s <- get
    put s{replyFs = (rf:replyFs s)}

    lift $ processPoints 0 payload' (dayMaps s) origin'

    now <- liftIO getCurrentTime

    if batch_period `addUTCTime` (start s) < now
        then get >>= yield
        else processMessage batch_period

processPoints :: MonadState BatchState m
              => Word64 -> ByteString -> (DayMap, DayMap) -> Origin -> m ()
processPoints offset message day_maps origin
    | fromIntegral offset >= BS.length message = return ()
    | otherwise = do
        let (address, time, payload) = runUnpacking (parseMessageAt offset) message
        let (simple_epoch, simple_buckets) = lookupBoth time (fst day_maps)

        let masked_address = address `clearBit` 0
        let simple_bucket = masked_address `mod` simple_buckets

        -- The LSB of the address lets us know if it is an extended message or
        -- not. Set means extended.
        if address `testBit` 0
            then do
                let len = fromIntegral payload
                let str = runUnpacking (getBytesAt (offset + 24) len) message
                let (ext_epoch, ext_buckets) = lookupBoth time (fst day_maps)
                let ext_bucket = masked_address `mod` ext_buckets
                appendExtended ext_epoch ext_bucket address time len str
                processPoints (offset + 24 + len) message day_maps origin
            else do
                let message_bytes = runUnpacking (getBytesAt offset 24) message
                appendSimple simple_epoch simple_bucket message_bytes
                processPoints (offset + 24) message day_maps origin

parseMessageAt :: Word64 -> Unpacking (Address, Time, Payload)
parseMessageAt offset = do
    unpackSetPosition (fromIntegral offset)
    (,,) <$> getWord64LE <*> getWord64LE <*> getWord64LE

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
    let simple_map = HashMap.lookupDefault HashMap.empty epoch (normal s)
    let simple_map' = HashMap.insertWith (flip (<>)) bucket builder simple_map
    let normal' = HashMap.insert epoch simple_map' (normal s)
    put $ s { normal = normal' }

appendExtended :: MonadState BatchState m
               => Epoch -> Bucket -> Address -> Time -> Word64 -> ByteString -> m ()
appendExtended epoch bucket address time len string = do
    s <- get

    -- First we write to the simple bucket, inserting a closure that will
    -- return a builder given an offset of the extended bucket write.
    let pending_map = HashMap.lookupDefault HashMap.empty epoch (pending s)

    -- Starting from zero, we write to the current offset and point the next
    -- extended point to the end of that write.
    let (os, fs) = HashMap.lookupDefault (0, []) bucket pending_map
    let os' = os + len

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

write :: Consumer BatchState Daemon ()
write = undefined

-- let oid = BS.pack $ printf "02_%s_%020d_%020d" (BS.unpack origin) bucket epoch
