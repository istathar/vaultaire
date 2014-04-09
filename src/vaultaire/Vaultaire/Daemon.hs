{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

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
    Response(..),
    Message(..),
    -- * Functions
    runDaemon,
    liftPool,
    nextMessage,
    refreshOriginDays,
    withDayFileLock,
) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State
import Data.Word (Word64)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List.NonEmpty (fromList)
import Vaultaire.Util(linkThread)
import Vaultaire.DayMap
import Vaultaire.OriginMap
import System.Rados.Monadic
import qualified System.ZMQ4.Monadic as ZMQ

-- User facing API

-- | The 'Daemon' monad is implemented transformer stack storing the per
-- origin DayMaps and channels for message retrieval and reply.
-- The underlying base monad is a rados 'Pool', you can lift to this via 'liftPool'.
newtype Daemon a = Daemon (StateT OriginDays (ReaderT DaemonConfig Pool) a)
  deriving ( Functor, Applicative, Monad, MonadIO, MonadReader DaemonConfig,
             MonadState OriginDays)

data DaemonConfig = DaemonConfig
    { messagesIn :: TBQueue Message
    , ackChan    :: TBQueue Ack
    }

-- | An acknowledgement of a message recieved, this will be attempted to be
-- delivered back to the sender of a 'Message'
data Response = Success            -- ^ Signifies to the client to not
                                   --   retransmit.
              | Failure ByteString -- ^ Only sent in response to an invalid
                                   --   message that will never be accepted.

-- | Represents a request made by a client. This could be a request to write a
-- point or a query.
--
-- All mesages follow the same asyncronous response, reply pattern.
data Message = Message
    { replyF  :: Response -> Daemon () -- ^ Queue a reply to this message. This
                                       --   will be transmitted automatically
                                       --   at a later point.
    , payload :: ByteString
    }

-- | This will go as far as to connect to Ceph and begin listening for
-- messages.
runDaemon :: String           -- ^ Broker for ZMQ
          -> Maybe ByteString -- ^ Username for Ceph
          -> ByteString       -- ^ Pool name for Ceph
          -> Daemon a
          -> IO a
runDaemon broker ceph_user pool (Daemon a) = do
    msg_chan <- atomically $ newTBQueue 4
    ack_chan <- atomically $ newTBQueue 16
    linkThread $ messenger broker msg_chan ack_chan

    runConnect ceph_user (parseConfig "/etc/ceph/ceph.conf") . runPool pool $
        runReaderT (evalStateT a emptyOriginMap) (DaemonConfig msg_chan ack_chan)

-- | Lift an action from the librados 'Pool' monad.
liftPool :: Pool a -> Daemon a
liftPool = Daemon . lift . lift

-- | Pop the next message off an internal FIFO queue of messages.
nextMessage :: Daemon Message
nextMessage = messagesIn <$> ask >>= liftIO . atomically . readTBQueue

-- | Ensure that the 'DayMap' for a given 'Origin' is up to date. If you need
-- the day map to be up to date for the entirity of an operation you must use
-- this within a 'withDayFileLock'.
refreshOriginDays :: Origin -> Daemon ()
refreshOriginDays origin = do
    om <- get
    -- If we already have it, reload if modified. Otherwise we just reload.
    case originLookup om origin of 
        Just (file_size, _) -> do
            let day_file = dayOID origin
            st <- liftPool $ runObject day_file stat
            case st of
                Left e -> error $ "Failed to stat day file: " ++ show day_file
                                  ++ "( " ++ show e ++ ")"
                Right result ->
                    unless (fileSize result == file_size) (reload om)
        Nothing -> reload om
  where
    reload om = do
        day_map <- liftPool $ dayMapFromCeph origin
        put $ originInsert om origin day_map

withDayFileLock :: Daemon a -> Daemon a
withDayFileLock = undefined

-- Internal

type FileSize = Word64
type OriginDays = OriginMap (FileSize, DayMap)
type Envelope = (ByteString, ByteString, ByteString)
data Ack = Ack Envelope Response

-- | Load a DayMap from Ceph, throwing errors on failure.
--
-- The file size is returned along side the map for cache invalidation.
dayMapFromCeph :: Origin -> Pool (FileSize, DayMap)
dayMapFromCeph origin = do
    day_file <- runObject (dayOID origin) readFull 
    case day_file of
        Left e -> error $ "Failed to read day file: " ++ show day_file ++
                          " (" ++ show e ++ ")"
        Right file -> tryLoad file day_file
  where
    tryLoad file day_file = case loadDayMap file of
        Left e -> error $ "Failed to load day file: " ++ show day_file ++
                            " (" ++ e ++ ")"
        Right day_map -> return (fromIntegral (BS.length file), day_map)

dayOID :: Origin -> ByteString
dayOID origin = "02_" `BS.append` origin `BS.append` "_days"

writeQueue :: MonadIO m => TBQueue a -> a -> m ()
writeQueue q = liftIO . atomically . writeTBQueue q
    
-- | Queue a response to a ZMQ message. The Envelope contains three idents,
--   which will allow the message first to reach the broker, then the client,
--   then finally for the client to distinguish these messages from each-other.
respond :: Envelope -> Response -> Daemon ()
respond env resp = do
    ack_chan <- ackChan <$> ask
    writeQueue ack_chan (Ack env resp)

messenger :: String
          -> TBQueue Message
          -> TBQueue Ack
          -> IO ()
messenger broker msg_chan ack_chan = ZMQ.runZMQ $ do
    router <- ZMQ.socket ZMQ.Router
    ZMQ.setReceiveHighWM (ZMQ.restrict (0 :: Int)) router
    ZMQ.connect router broker
    listen router msg_chan ack_chan

-- | Listen for messages, multiplexing incoming and outgoing over the same
-- socket.
listen :: ZMQ.Socket z ZMQ.Router
       -> TBQueue Message
       -> TBQueue Ack
       -> ZMQ.ZMQ z ()
listen router msg_chan ack_chan = forever $ do
    result <- ZMQ.poll 100 [ZMQ.Sock router [ZMQ.In] Nothing]
    case result of
        -- Message waiting
        [[ZMQ.In]] -> do
            msg <- ZMQ.receiveMulti router
            case msg of
                [env_a, env_b, message_id, payload'] -> do
                    let respond_f = respond (env_a, env_b, message_id)
                    writeQueue msg_chan $ Message respond_f payload'
                n -> liftIO $ putStrLn $
                    "bad message recieved, " ++ show (length n)
                    ++ " parts; ignoring"
        -- Timeout, do nothing.
        [[]]        -> return ()
        _           -> error "daemon listen: unpossible"

    -- Send all acks every iteration
    sendAcks router ack_chan

responseToPayload :: Response -> ByteString
responseToPayload Success = ""        -- An empty response signifies success
responseToPayload (Failure msg) = msg -- Any response signifieds failure

-- This depletes the entire queue of messages.
sendAcks :: ZMQ.Socket z ZMQ.Router -> TBQueue Ack -> ZMQ.ZMQ z ()
sendAcks router ack_chan = do
    ack <- liftIO $ atomically $ tryReadTBQueue ack_chan
    case ack of
        Nothing -> return ()
        Just (Ack (env_a, env_b, message_id) response') -> do
            let payload' = responseToPayload response'
            let reply = fromList [env_a, env_b, message_id, payload']
            ZMQ.sendMulti router reply
            sendAcks router ack_chan
