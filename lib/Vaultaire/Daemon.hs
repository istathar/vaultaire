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
    handleMessages,
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
    dayMapsFromCeph,
    simpleDayOID,
    extendedDayOID,
    bucketOID,
    withPool,
) where

import Control.Applicative
import Control.Concurrent.Async (Async)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.List.NonEmpty (fromList)
import Data.Maybe
import Data.Word (Word64)
import System.Log.Logger
import System.Rados.Monadic (Pool, fileSize, parseConfig, readFull,
                             runConnect, runObject, runObject, runPool, stat,
                             withExclusiveLock, withSharedLock)
import qualified System.Rados.Monadic as Rados
import qualified System.ZMQ4 as ZMQ
import Text.Printf
import Vaultaire.DayMap
import Vaultaire.OriginMap
import Vaultaire.Types
import Vaultaire.Util

-- User facing API

-- | The 'Daemon' monad stores per 'Origin' 'DayMap's and queues for message
-- retrieval and reply. The underlying base monad is a rados 'Pool', you can
-- lift to this via 'liftPool'.
newtype Daemon a = Daemon (StateT OriginDays (ReaderT SharedConnection Pool) a)
  deriving ( Functor, Applicative, Monad, MonadIO, MonadReader SharedConnection,
             MonadState OriginDays)

-- | Handle to commuicate with the 0MQ router.
type SharedConnection = MVar (ZMQ.Socket ZMQ.Router)

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

-- | Handle messages using an arbitrary concurrency abstraction.
--
-- In order for this to behave, your message handling function must be
-- stateless, there is no guarantee that it will be run in the same thread,
-- thus no assumptions should be made about the DayMap from a previous request
-- sticking around.
--
-- This prohibits any multi-message requests, if this is what you want you had
-- best define your own concurrency mechanism.
handleMessages :: String                 -- ^ Broker for ZMQ
               -> Maybe ByteString       -- ^ Username for Ceph
               -> ByteString             -- ^ Pool name for Ceph
               -> MVar ()                -- ^ Shutdown signal
               -> (Message -> Daemon ()) -- ^ Message handling function
               -> IO ()
handleMessages broker ceph_user pool shutdown f =
    runDaemon broker ceph_user pool loop
  where
    -- Dumb, no concurrency for now.
    loop = do
        done <- isJust <$> liftIO (tryReadMVar shutdown)
        unless done $ do
            maybe_next <- nextMessage
            case maybe_next of
                Nothing -> loop
                Just msg -> f msg >> loop

-- | This will go as far as to connect to Ceph and begin listening for
-- messages.
runDaemon :: String           -- ^ Broker for ZMQ
          -> Maybe ByteString -- ^ Username for Ceph
          -> ByteString       -- ^ Pool name for Ceph
          -> Daemon a
          -> IO a
runDaemon broker ceph_user pool (Daemon a) =
    bracket (setupSharedConnection broker)
            (\(ctx, conn) -> do
                sock <- takeMVar conn
                ZMQ.close sock
                ZMQ.shutdown ctx)
            (\(_, conn) ->
                withPool ceph_user pool $
                    runReaderT (evalStateT a emptyOriginMap) conn)

-- Connect to ceph and run your pool action
withPool :: Maybe ByteString -> ByteString -> Pool a -> IO a
withPool ceph_user pool = runConnect ceph_user (parseConfig "/etc/ceph/ceph.conf") . runPool pool

-- | Lift an action from the librados 'Pool' monad.
liftPool :: Pool a -> Daemon a
liftPool = Daemon . lift . lift

-- | Pop the next message off an internal FIFO queue of messages.
--   Incoming message should be four parts:
--   1. The routing information back to the broker.
--   2. The routing information back to the client, from the broker.
--   3. The the origin, unverified and unauthenticated for now.
--   4. The client's payload.
nextMessage :: Daemon (Maybe Message)
nextMessage = do
    conn <- ask
    liftIO $ withMVar conn $ \c -> do
        result <- ZMQ.poll 10 [ZMQ.Sock c [ZMQ.In] Nothing]
        case result of
            -- Message waiting
            [[ZMQ.In]] -> do
                msg <- doRecv c

                case msg of
                    -- Invalid message
                    Nothing -> return Nothing
                    Just (env_a, env_b, origin, payload) ->
                        -- This can be moved out of a lambda when I fully understand this:
                        -- http://www.haskell.org/pipermail/haskell-cafe/2012-August/103041.html
                        let send r = flip ZMQ.sendMulti (fromList [env_a, env_b, toWire r])
                        in return . Just $
                            Message (\r -> do var <- ask
                                              liftIO $ withMVar var (send r))
                                    (Origin origin)
                                    payload
            -- Timeout, do nothing.
            [[]]        -> return Nothing
            _           -> fatal "Daemon.listen" "impossible"
  where
    doRecv sock =  do
        msg <- ZMQ.receiveMulti sock
        case msg of
            [env_a, env_b, origin, payload] ->
                return . Just $ (env_a, env_b, origin, payload)
            n -> do
                liftIO . errorM "Daemon.nextMessage" $
                                "bad message recieved, " ++ show (length n)
                                ++ " parts; ignoring"
                return Nothing

-- | Run an action in the 'Control.Concurrent.Async' monad.
-- State will be empty and completely separated from any other thread. This is
-- to avoid strange memory leaks and complexity.
--
-- You do however have access to the same messaging channels, so sending and
-- receiving messages will work fine and is thread safe.
async :: Daemon a -> Daemon (Async a)
async (Daemon a) = do
    -- TODO: Handle waiting for any 'child' threads created, as the underlying
    --       connection is now shared.
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

-- | Ensure that the 'DayMap's for a given 'Origin' are up to date.
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

-- | Build the 'SharedConnection' for use by potentially many consumers within
-- this 'Daemon'.
setupSharedConnection :: String -- ^ Broker name
                      -> IO (ZMQ.Context, SharedConnection)
setupSharedConnection broker = do
    ctx <- ZMQ.context
    sock <- ZMQ.socket ctx ZMQ.Router
    ZMQ.connect sock broker
    mvar <- newMVar sock
    return (ctx, mvar)

bucketOID :: Origin -> Epoch -> Bucket -> String -> ByteString
bucketOID (Origin origin') epoch bucket kind = BS.pack $ printf "02_%s_%020d_%020d_%s"
                                                         (BS.unpack origin')
                                                         bucket
                                                         epoch
                                                         kind
