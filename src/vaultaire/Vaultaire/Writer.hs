{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Writer
(
    startWriter,
) where

import Vaultaire.Daemon
import Vaultaire.RollOver
import Vaultaire.OriginMap
import Vaultaire.Util(linkThread)
import Data.Bits
import qualified Data.ByteString.Char8 as BS
import Data.Maybe
import Data.ByteString(ByteString)
import Data.ByteString.Lazy.Builder
import Control.Applicative
import Data.Packer
import Control.Monad
import Control.Concurrent.STM
import Data.Time
import Control.Monad.State.Strict
import Pipes
import Pipes.Lift
import Pipes.Concurrent
import qualified Control.Concurrent.Async as Async
import Control.Concurrent(threadDelay)
import Data.Word(Word64)
import Vaultaire.DayMap
import Data.Monoid
import Text.Printf
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap

data Stream = Packet Message | Tick

data Write = Write
    { simple   :: Builder
    , extended :: [Word64 -> Builder]
    , strings  :: Builder
    }

type Address = Word64
type Payload = Word64

type FlyingThreadMonsterMap = OriginMap (Output Message)

type EpochMap = HashMap

data BatchState = BatchState
    { replyFs  :: [Response -> Daemon ()]
    , normal   :: HashMap Epoch Builder
    , pending  :: HashMap Epoch [Word64 -> Builder]
    , string   :: HashMap Epoch Builder
    , dayMap   :: DayMap
    , start    :: UTCTime
    }

tickRate :: Int
tickRate = 1000000000 `div` 100

-- | Start a writer daemon, never returns.
startWriter :: String           -- ^ Broker
            -> Maybe ByteString -- ^ Username for Ceph
            -> ByteString       -- ^ Pool name for Ceph
            -> IO ()
startWriter b u p = runDaemon b u p $ do
    runEffect $ lift nextMessage >~ evalStateP emptyOriginMap flyingThreadMonster
    error "startWriter: impossible"

-- If the incoming message currently has a processing thread running for that
-- origin, feed this into that 'thread iteration'. Otherwise create a new one
-- and feed it in.
--
-- This thread will take care of writing its accumulated things to Ceph when done.
flyingThreadMonster :: Consumer Message (StateT FlyingThreadMonsterMap Daemon) ()
flyingThreadMonster = do
    m@(Message _ origin' _) <- await
    ftm <- lift get
    case originLookup origin' ftm of
        Just output -> do
            sent <- send' output m
            -- If it wasn't sent, the thread has shut itself down.
            if sent
                then startThread origin' ftm m
                else flyingThreadMonster
        Nothing   -> startThread origin' ftm m
  where
    send' o = liftIO . atomically . send o

    startThread origin' ftm m = do
        (output, seal, input) <- liftIO $ spawn' (Bounded 16)
        lift . lift $ async (processBatch origin' seal input)
        must_send <- send' output m
        unless must_send $ error "ftm died immediately, bailing"
        lift . put $ originInsert origin' output ftm
        flyingThreadMonster

batchPeriod = 10 -- seconds

batchStateNow :: DayMap -> IO BatchState
batchStateNow dm = do
    BatchState mempty mempty mempty mempty dm <$> getCurrentTime

-- | The flying thread monster has done the hard work for us and sorted
-- incoming bursts by origin. Now we simply need to process these messages with
-- local state, writing to ceph when our collection time has elapsed.
processBatch :: Origin -> Input Message -> STM () -> Daemon ()
processBatch origin' input seal = do
    refreshOriginDays origin'
    maybe_dm <- originDayMap origin'
    case maybe_dm of
        Nothing ->
            -- Reply to first message with an error, try again on the next
            -- message. This is a potential DOS of sorts, however is needed
            -- unless we know when to expire 
            runEffect $ fromInput input >-> badOrigin
        Just dm -> do
            -- Process for a batch period
            start_state <- liftIO $ batchStateNow dm 
            runEffect $ fromInput input >-> evalStateP start_state processMessage >-> write
            liftIO $ atomically seal

badOrigin :: Consumer Message Daemon ()
badOrigin = do
    Message reply_f _ _ <- await
    lift $ reply_f $ Failure "No such origin"


feedTicks :: Producer Stream IO b
feedTicks = forever $ (lift $ threadDelay tickRate) >> yield Tick

-- | Place a message into state, wherever it belongs.
processMessage :: Pipe Message BatchState (StateT BatchState Daemon) ()
processMessage = do
    -- Most messages simply need to be placed into the correct epoch and
    -- bucket, extended ones are a little more complex in that they have to be
    -- stored as an offset to a pending write to the extended buckets.
    Message rf origin' payload' <- await


    -- Append the replyf for this message, maybe a lense would be nice
    BatchState{..} <- get
    let state' = BatchState (rf:replyFs) normal pending string dayMap start
    put state'

    lift $ processPoints 0 payload' dayMap origin'
    
    now <- liftIO getCurrentTime

    if batchPeriod `addUTCTime` start > now
        then get >>= yield
        else processMessage

processPoints :: Int -> ByteString -> DayMap -> Origin -> (StateT BatchState Daemon) ()
processPoints offset message day_map origin
    | BS.length message >= offset = return ()
    | otherwise = do
        let (address, time, payload) = runUnpacking (parseMessageAt offset) message
        let (epoch, n_buckets) = lookupBoth time day_map

        let masked_address = address `clearBit` 0
        let bucket = masked_address `mod` n_buckets

        -- The LSB of the address lets us know if it is an extended message or
        -- not. Set means extended.
        if address `testBit` 0
            then appendSimple epoch masked_address time payload
            else appendExtended epoch masked_address time payload

parseMessageAt :: Int -> Unpacking (Address, Time, Payload)
parseMessageAt offset = do
    unpackSetPosition offset
    (,,) <$> getWord64LE <*> getWord64LE <*> getWord64LE

appendSimple :: Epoch -> Address -> Time -> Payload -> (StateT BatchState Daemon) ()
appendSimple = undefined
    
appendExtended :: Epoch -> Address -> Time -> Payload -> (StateT BatchState Daemon) ()
appendExtended = undefined
    
write :: Consumer BatchState Daemon ()
write = undefined

-- let oid = BS.pack $ printf "02_%s_%020d_%020d" (BS.unpack origin) bucket epoch
