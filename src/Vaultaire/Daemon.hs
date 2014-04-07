{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies               #-}

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
    Message,
    runDaemon,
)

where
import Control.Applicative
import Control.Concurrent.Async
import Control.Monad.IO.Class
import Control.Monad.Layer
import Control.Monad.Reader hiding (lift)
import Data.ByteString (ByteString)
import Pipes hiding (lift)
import qualified Pipes
import Pipes.Concurrent
import qualified System.Rados.Monadic as Rados
import qualified System.ZMQ4.Monadic as ZMQ

data Response = Success | Failure ByteString
type Message = (ByteString, Response -> Daemon ())

newtype Daemon a = Daemon
    { unDaemon :: ReaderT (Input Message) Rados.Pool a}
  deriving (Functor, Applicative, Monad, MonadIO)

instance MonadLayer Daemon where
    type Inner Daemon = ReaderT (Input Message) Rados.Pool
    layer = Daemon . lift
    layerInvmap = const . layerMap

instance MonadLayerFunctor Daemon where
    layerMap f (Daemon a) = Daemon (f a)

runDaemon :: String           -- | ^ Broker for ZMQ
          -> Maybe ByteString -- | ^ Username for Ceph
          -> ByteString       -- | ^ Pool name for Ceph
          -> Daemon a
          -> IO a
runDaemon broker ceph_user pool (Daemon a) = do
    (output, input) <- spawn Unbounded
    lift (startAsync $ messenger broker output)
    Rados.runConnect ceph_user (Rados.parseConfig "/etc/ceph/ceph.conf") $
        Rados.runPool pool $
            runReaderT a input

startAsync :: IO a -> IO ()
startAsync a = async a >>= link

messenger :: String -> Output Message -> IO ()
messenger broker output = ZMQ.runZMQ $ do
    router <- ZMQ.socket ZMQ.Router
    ZMQ.setReceiveHighWM (ZMQ.restrict (0 :: Int)) router
    ZMQ.connect router ("tcp://" ++ broker ++ ":5571")
    runEffect $ readMessages router >-> toOutput output

readMessages :: ZMQ.Socket z ZMQ.Router -> Producer Message (ZMQ.ZMQ z) ()
readMessages router = forever $ do
    msg <- Pipes.lift $ ZMQ.receiveMulti router
    case length msg of
        4 -> yield (msg !! 3, undefined)
        n -> Pipes.liftIO $ putStrLn $
            "bad message recieved, "++ show n ++" parts; ignoring"

