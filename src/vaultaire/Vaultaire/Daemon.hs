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
    incomingMessages,
) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.STM.TBChan
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.ByteString (ByteString)
import Data.List.NonEmpty (fromList)
import Pipes
import Pipes.Concurrent
import qualified System.Rados.Monadic as Rados
import qualified System.ZMQ4.Monadic as ZMQ

-- User facing API

newtype Daemon a = Daemon
    { unDaemon :: ReaderT DaemonConfig Rados.Pool a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader DaemonConfig)

data DaemonConfig = DaemonConfig
    { messagesIn :: Input Message
    , ackChan    :: TBChan Ack
    }

data Response = Success | Failure ByteString

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
    (msgs_out, msgs_in) <- spawn (Bounded 16)
    (ack_chan) <- liftIO (newTBChanIO 128)
    -- TODO: Thread errors over telemetry channel
    let error_f = putStrLn
    startAsync $ messenger broker msgs_out ack_chan error_f

    Rados.runConnect ceph_user (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool $
            runReaderT a (DaemonConfig msgs_in ack_chan)

liftPool :: Rados.Pool a -> Daemon a
liftPool = Daemon . lift

incomingMessages :: Producer Message Daemon ()
incomingMessages = messagesIn <$> ask >>= fromInput

-- Internal

type ErrorF = (String -> IO ())
type Envelope = (ByteString, ByteString, ByteString)
data Ack = Ack Envelope Response

respond :: Envelope -> Response -> Daemon ()
respond env resp = do
    ack_chan <- ackChan <$> ask
    liftIO $ atomically $ writeTBChan ack_chan (Ack env resp)

startAsync :: IO a -> IO ()
startAsync a = async a >>= link

messenger :: String
          -> Output Message
          -> TBChan Ack
          -> ErrorF
          -> IO ()
messenger broker msgs_out ack_chan error_f = ZMQ.runZMQ $ do
    router <- ZMQ.socket ZMQ.Router
    ZMQ.setReceiveHighWM (ZMQ.restrict (0 :: Int)) router
    ZMQ.connect router broker
    runEffect $ listen router ack_chan error_f >-> toOutput msgs_out

listen :: ZMQ.Socket z ZMQ.Router
       -> TBChan Ack
       -> ErrorF
       -> Producer Message (ZMQ.ZMQ z) ()
listen router ack_chan error_f = forever $ do
    result <- ZMQ.poll 100 [ZMQ.Sock router [ZMQ.In] Nothing]
    case result of
        -- Message waiting
        [[ZMQ.In]] -> do
            msg <- lift $ ZMQ.receiveMulti router
            case msg of
                [env_a, env_b, message_id, payload'] -> do
                    let respond_f = respond (env_a, env_b, message_id)
                    yield $ Message respond_f payload'
                n -> liftIO $ error_f $
                    "bad message recieved, " ++ show n ++ " parts; ignoring"
        -- Timeout, do nothing.
        [[]]        -> return ()
        _           -> error "daemon listen: unpossible"

    lift $ sendAcks router ack_chan

responseToPayload :: Response -> ByteString
responseToPayload Success = ""
responseToPayload (Failure msg) = msg

sendAcks :: ZMQ.Socket z ZMQ.Router -> TBChan Ack -> ZMQ.ZMQ z ()
sendAcks router ack_chan = do
    ack <- liftIO $ atomically $ tryReadTBChan ack_chan
    case ack of
        Nothing -> return ()
        Just (Ack (env_a, env_b, message_id) response') -> do
            let payload' = responseToPayload response'
            let reply = fromList [env_a, env_b, message_id, payload']
            ZMQ.sendMulti router reply
            sendAcks router ack_chan
