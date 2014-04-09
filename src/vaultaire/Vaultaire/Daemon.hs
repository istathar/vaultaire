{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

-- | Encapsulate runtime requirements of a generic vaultaire daemon within a
-- monad.
--
-- Handles:
-- * connection to ceph,
-- * storage of a polymorphic config, and
-- * message retrieval/reply.
module Vaultaire.Daemon
(
    Daemon(..),
    Response(..),
    Message(..),
    runDaemon,
    liftPool,
    nextMessage,
    refreshOriginDays,
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
import qualified System.Rados.Monadic as Rados
import qualified System.ZMQ4.Monadic as ZMQ

-- User facing API

newtype Daemon a = Daemon
    { unDaemon :: StateT OriginDays (ReaderT DaemonConfig Rados.Pool) a}
  deriving (
    Functor,
    Applicative,
    Monad,
    MonadIO,
    MonadReader DaemonConfig,
    MonadState OriginDays
  )

data DaemonConfig = DaemonConfig
    { messagesIn :: TBQueue Message
    , ackChan    :: TBQueue Ack
    }

data Response = Success | Failure ByteString
  deriving (Show, Eq)

data Message = Message
    { replyF  :: Response -> Daemon ()
    , payload :: ByteString
    }

runDaemon :: String           -- ^ Broker for ZMQ
          -> Maybe ByteString -- ^ Username for Ceph
          -> ByteString       -- ^ Pool name for Ceph
          -> Daemon a
          -> IO a
runDaemon broker ceph_user pool (Daemon a) = do
    msg_chan <- atomically $ newTBQueue 4
    ack_chan <- atomically $ newTBQueue 16
    -- TODO: Thread errors over telemetry channel
    let error_f = putStrLn
    linkThread $ messenger broker msg_chan ack_chan error_f

    Rados.runConnect ceph_user (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool $
            let config = DaemonConfig msg_chan ack_chan
            in runReaderT (evalStateT a emptyOriginMap) config

liftPool :: Rados.Pool a -> Daemon a
liftPool = Daemon . lift . lift

nextMessage :: Daemon Message
nextMessage = messagesIn <$> ask >>= liftIO . atomically . readTBQueue

refreshOriginDays :: Origin -> Daemon ()
refreshOriginDays origin = do
    om <- get
    -- If the lookup succeeds, we reload if changed, otherwise we just reload.
    case originLookup om origin of 
        Just (file_size, _) -> do
            let day_file = dayOID origin
            st <- doStat day_file
            case st of
                Left e -> error $ "Failed to stat day file: " ++ show day_file
                                  ++ "( " ++ show e ++ ")"
                Right result ->
                    unless (Rados.fileSize result == file_size) (reload om)
        Nothing -> reload om
  where
    reload om = do
        day_map <- liftPool $ dayMapFromCeph origin
        put $ originInsert om origin day_map

    doStat oid =
        liftPool $ Rados.runObject oid Rados.stat
        


-- Internal

type FileSize = Word64
type OriginDays = OriginMap (FileSize, DayMap)
type ErrorF = (String -> IO ())
type Envelope = (ByteString, ByteString, ByteString)
data Ack = Ack Envelope Response

dayMapFromCeph :: Origin -> Rados.Pool (FileSize, DayMap)
dayMapFromCeph origin = do
    day_file <- Rados.runObject (dayOID origin) Rados.readFull 
    case day_file of
        Left e -> error $ "Failed to read day file: " ++ show day_file ++
                          " (" ++ show e ++ ")"
        Right file -> case loadDayMap file of
            Left e -> error $ "Failed to load day file: " ++ show day_file ++
                               " (" ++ e ++ ")"
            Right day_map -> return (fromIntegral (BS.length file), day_map)
            

dayOID :: Origin -> ByteString
dayOID origin = "02_" `BS.append` origin `BS.append` "_days"
    

respond :: Envelope -> Response -> Daemon ()
respond env resp = do
    ack_chan <- ackChan <$> ask
    liftIO $ atomically $ writeTBQueue ack_chan (Ack env resp)

messenger :: String
          -> TBQueue Message
          -> TBQueue Ack
          -> ErrorF
          -> IO ()
messenger broker msg_chan ack_chan error_f = ZMQ.runZMQ $ do
    router <- ZMQ.socket ZMQ.Router
    ZMQ.setReceiveHighWM (ZMQ.restrict (0 :: Int)) router
    ZMQ.connect router broker
    listen router msg_chan ack_chan error_f

writeQueue :: MonadIO m => TBQueue Message -> Message -> m ()
writeQueue q = liftIO . atomically . writeTBQueue q

listen :: ZMQ.Socket z ZMQ.Router
       -> TBQueue Message
       -> TBQueue Ack
       -> ErrorF
       -> ZMQ.ZMQ z ()
listen router msg_chan ack_chan error_f = forever $ do
    result <- ZMQ.poll 100 [ZMQ.Sock router [ZMQ.In] Nothing]
    case result of
        -- Message waiting
        [[ZMQ.In]] -> do
            msg <- ZMQ.receiveMulti router
            case msg of
                [env_a, env_b, message_id, payload'] -> do
                    let respond_f = respond (env_a, env_b, message_id)
                    writeQueue msg_chan $ Message respond_f payload'
                n -> liftIO $ error_f $
                    "bad message recieved, " ++ show (length n)
                    ++ " parts; ignoring"
        -- Timeout, do nothing.
        [[]]        -> return ()
        _           -> error "daemon listen: unpossible"

    sendAcks router ack_chan

responseToPayload :: Response -> ByteString
responseToPayload Success = ""
responseToPayload (Failure msg) = msg

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
