{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE Rank2Types                 #-}

-- | Encapsulates runtime requirements of a generic vaultaire daemon
--
-- Handles:
--
-- * connection to ceph,
--
-- * message retrieval/reply.
--
-- * caching of an Origin specific DayMap
module Vaultaire.Daemon
(
    -- * Types
    Daemon,
    Message(..),
    ReplyF,
    Address(..),
    Payload,
    Bucket,
    -- * Functions
    runDaemon,
    liftPool,
    nextMessage,
    async,
    refreshOriginDays,
    withSimpleDayMap,
    withExtendedDayMap,
    withLock,
    withExLock,
    cacheExpired,
    -- * Helpers
    simpleDayOID,
    extendedDayOID,
    bucketOID
) where

import Control.Applicative
import Control.Concurrent (ThreadId, killThread, myThreadId)
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.List.NonEmpty (NonEmpty, fromList)
import Data.Word (Word64)
import System.Log.Logger
import System.Rados.Monadic (Pool, fileSize, parseConfig, readFull,
                             runConnect, runObject, runObject, runPool, stat,
                             withExclusiveLock, withSharedLock)
import qualified System.Rados.Monadic as Rados
import qualified System.ZMQ4.Monadic as ZMQ
import Text.Printf
import Vaultaire.DayMap
import Vaultaire.OriginMap
import Vaultaire.Types
import Vaultaire.Util

-- User facing API

-- | The 'Daemon' monad stores per 'Origin' 'DayMap's and queues for message
-- retrieval and reply. The underlying base monad is a rados 'Pool', you can
-- lift to this via 'liftPool'.
newtype Daemon a = Daemon (StateT OriginDays (ReaderT DaemonConfig Pool) a)
  deriving ( Functor, Applicative, Monad, MonadIO, MonadReader DaemonConfig,
             MonadState OriginDays)

data DaemonConfig = DaemonConfig
    { recvChan :: TBQueue RawMsg
    , respChan :: TBQueue (NonEmpty ByteString)
    }

-- Simple and extended day maps
type OriginDays = OriginMap ((FileSize, DayMap), (FileSize, DayMap))

-- | Represents a request made by a client. This could be a request to write a
-- point or a query.
--
-- All mesages follow the same asyncronous response, reply pattern.
data Message = Message
    { messageReplyF  :: ReplyF -- ^ Queue a reply to this message. This
                        --   will be transmitted automatically
                        --   at a later point.
    , messageOrigin  :: Origin
    , messagePayload :: ByteString
    }

type ReplyF  = WireFormat w => w -> Daemon ()
type Payload = Word64
type Bucket  = Word64
type RawMsg  = (ByteString, ByteString, ByteString, ByteString, ByteString)

-- | This will go as far as to connect to Ceph and begin listening for
-- messages.
runDaemon :: String           -- ^ Broker for ZMQ
          -> Maybe ByteString -- ^ Username for Ceph
          -> ByteString       -- ^ Pool name for Ceph
          -> Daemon a
          -> IO a
runDaemon broker ceph_user pool (Daemon a) = do
    msg_chan <- atomically $ newTBQueue 4
    resp_chan <- atomically $ newTBQueue 16

    parent_tid <- myThreadId
    messenger_a <- Async.async $ messenger broker msg_chan resp_chan
    -- Ensure that any exceptions are re-thrown in a third thread.
    monitor_a <- Async.async $ monitorMessenger messenger_a parent_tid

    r <- runConnect ceph_user (parseConfig "/etc/ceph/ceph.conf") . runPool pool $
        runReaderT (evalStateT a emptyOriginMap) (DaemonConfig msg_chan resp_chan)

    Async.cancel monitor_a
    Async.cancel messenger_a

    return r

-- | Handle messsenger thread shutting down or throwing an exception
-- explicitly. On normal shutdown, this thread must be killed by the parent
-- thread before the messenger thread is shutdown..
monitorMessenger :: Async () -> ThreadId -> IO ()
monitorMessenger thread parent = do
    result <- Async.waitCatch thread
    case result of
        Left e ->
            errorM "Daemon.monitorMessenger" $
                   "Messenger thread exploded, killing parent: " ++ show e
        Right _ ->
            errorM "Daemon.monitorMessenger"
                   "Messenger thread exited, killing parent."

    killThread parent

-- | Lift an action from the librados 'Pool' monad.
liftPool :: Pool a -> Daemon a
liftPool = Daemon . lift . lift

-- | Pop the next message off an internal FIFO queue of messages.
nextMessage :: Daemon Message
nextMessage = do
    (env_a, env_b, env_c, origin, payload) <- getMsg
    -- This can be moved out of a lambda when I fully understand this:
    -- http://www.haskell.org/pipermail/haskell-cafe/2012-August/103041.html
    return $ Message (\r -> do resp_chan <- respChan <$> ask
                               writeQueue resp_chan (fromList [env_a, env_b, env_c, toWire r]))
                     (Origin origin)
                     payload
  where
    getMsg = recvChan <$> ask >>= liftIO . atomically . readTBQueue

-- | Run an action in the 'Control.Concurrent.Async' monad.
-- State will be empty and completely separated from any other thread. This is
-- to avoid strange memory leaks and complexity.
--
-- You do however have access to the same messaging channels, so sending and
-- receiving messages will work fine and is thread safe.
async :: Daemon a -> Daemon (Async a)
async (Daemon a) = do
    conf <- ask
    liftPool $ Rados.async (runReaderT (evalStateT a emptyOriginMap) conf)

-- | Fetch the simple day map for a given origin
withSimpleDayMap :: Origin -> (DayMap -> a) -> Daemon (Maybe a)
withSimpleDayMap origin' f = do
    om <- get
    return $ f . snd . fst <$> originLookup origin' om

-- | Fetch the extended day map for a given origin
withExtendedDayMap :: Origin -> (DayMap -> a) -> Daemon (Maybe a)
withExtendedDayMap origin' f = do
    om <- get
    return $ f . snd . snd <$> originLookup origin' om

-- | Ensure that the 'DayMap's for a given 'Origin' are up to date. If you need
-- the day map to be up to date for the entirity of an operation you must use
-- this within a 'withDayFileLock'.
refreshOriginDays :: Origin -> Daemon ()
refreshOriginDays origin' = do
    om <- get
    -- If we already have it, reload if modified. Otherwise we just reload.
    expired <- cacheExpired om origin'
    when expired $ reload om
  where
    reload om = do
        result <- liftPool $ dayMapsFromCeph origin'
        case result of
            Left e -> liftIO $ putStrLn e
            Right day_map -> put $ originInsert origin' day_map om

-- | Read this:
--
-- This function a little odd, due to my hesitancy adopting something cool like
-- layers, or even MonadCatchIO.
--
-- In order to grab a shared lock, we lift to the Pool monad, but to run the
-- user's action we must re-wrap the state.
--
-- TLDR: Daemon state within will not be updated within the 'outer' monad until
-- the entire action completes. You will probably never even notice this.
withLock :: ByteString -> Daemon a -> Daemon a
withLock oid = wrapPool (withSharedLock oid "lock" "lock" "daemon" Nothing)

-- | Same pitfalls as withLock, this one acquires an exclusive lock.
withExLock :: ByteString -> Daemon a -> Daemon a
withExLock oid = wrapPool (withExclusiveLock oid "lock" "lock" Nothing)

wrapPool :: (Pool (a, OriginDays) -> Pool (b, OriginDays))
         -> Daemon a -> Daemon b
wrapPool pool_a (Daemon a) = do
    conf <- ask
    st   <- get
    (r,s) <- liftPool $ pool_a (runReaderT (runStateT a st) conf)
    put s
    return r

-- Internal

type FileSize = Word64

-- | Check if a cached origin has expired.
cacheExpired :: OriginDays -> Origin -> Daemon Bool
cacheExpired om origin' =
    case originLookup origin' om of
        Just ((simple_size, _), (ext_size, _)) -> do
            simple_expired <- checkDayFile (simpleDayOID origin') simple_size
            if not simple_expired
                then checkDayFile (extendedDayOID origin') ext_size
                else return simple_expired
        Nothing -> return True
  where
    checkDayFile file expected_size = do
        st <- liftPool $ runObject file stat
        case st of
            Left e -> fatal "Daemon.cacheExpired" $
                            "Failed to stat day file: " ++ show file
                            ++ "( " ++ show e ++ ")"
            Right result -> return $ fileSize result /= expected_size


-- | Load a DayMap from Ceph
--
-- The file size is returned along side the map for cache invalidation.
dayMapsFromCeph :: Origin -> Pool (Either String ((FileSize, DayMap), (FileSize, DayMap)))
dayMapsFromCeph origin' = do
    simple <- tryRead (simpleDayOID origin')
    extended <- tryRead (extendedDayOID origin')
    return $ (,) <$> simple <*> extended
  where
    tryRead file =  do
        result <- runObject file readFull
        case result of
            Left e ->
                return $ Left $ "Failed to read day file: " ++ show file ++
                                " (" ++ show e ++ ")"
            Right contents ->
                tryLoad file contents
    tryLoad day_file contents = case loadDayMap contents of
        Left e ->
            return $ Left $ "Failed to load day file: " ++
                            show day_file ++ " (" ++ e ++ ")"
        Right day_map ->
            return $ Right (fromIntegral (BS.length contents), day_map)

simpleDayOID :: Origin -> ByteString
simpleDayOID (Origin origin') = "02_" `BS.append` origin' `BS.append` "_simple_days"

extendedDayOID :: Origin -> ByteString
extendedDayOID (Origin origin') = "02_" `BS.append` origin' `BS.append` "_extended_days"

writeQueue :: MonadIO m => TBQueue a -> a -> m ()
writeQueue q = liftIO . atomically . writeTBQueue q

messenger :: String
          -> TBQueue RawMsg
          -> TBQueue (NonEmpty ByteString)
          -> IO ()
messenger broker recv_chan resp_chan = ZMQ.runZMQ $ do
    router <- ZMQ.socket ZMQ.Router
    ZMQ.setReceiveHighWM (ZMQ.restrict (0 :: Int)) router
    ZMQ.connect router broker
    listen router recv_chan resp_chan

-- | Listen for messages, multiplexing incoming and outgoing over the same
-- socket.
listen :: ZMQ.Socket z ZMQ.Router
       -> TBQueue RawMsg
       -> TBQueue (NonEmpty ByteString)
       -> ZMQ.ZMQ z ()
listen router recv_chan resp_chan = forever $ do
    result <- ZMQ.poll 100 [ZMQ.Sock router [ZMQ.In] Nothing]
    case result of
        -- Message waiting
        [[ZMQ.In]] -> do
            msg <- ZMQ.receiveMulti router
            case msg of
                [env_a, env_b, message_id, origin', payload'] ->
                    writeQueue recv_chan (env_a, env_b, message_id, origin', payload')
                n -> liftIO $ putStrLn $
                    "bad message recieved, " ++ show (length n)
                    ++ " parts; ignoring"
        -- Timeout, do nothing.
        [[]]        -> return ()
        _           -> fatal "Daemon.listen" "impossible"

    -- Send all responses every iteration
    sendResponses router resp_chan

-- This depletes the entire queue of messages.
sendResponses :: ZMQ.Socket z ZMQ.Router -> TBQueue (NonEmpty ByteString) -> ZMQ.ZMQ z ()
sendResponses router resp_chan = do
    ack <- liftIO $ atomically $ tryReadTBQueue resp_chan
    case ack of
        Nothing -> return ()
        Just payload -> do
            ZMQ.sendMulti router payload
            sendResponses router resp_chan

bucketOID :: Origin -> Epoch -> Bucket -> String -> ByteString
bucketOID (Origin origin') epoch bucket kind = BS.pack $ printf "02_%s_%020d_%020d_%s"
                                                         (BS.unpack origin')
                                                         bucket
                                                         epoch
                                                         kind
