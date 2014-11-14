{-# LANGUAGE OverloadedStrings #-}

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.Trans.State
import qualified Data.List as L
import           Data.Maybe
import           Network.URI
import           System.ZMQ4 hiding (shutdown)
import qualified System.ZMQ4.Monadic as Z
import           Test.Hspec hiding (pending)

import           Vaultaire.Daemon
import           Vaultaire.Broker
import           Vaultaire.Writer
import           Vaultaire.Profiler
import           Vaultaire.Types
import           Vaultaire.Util
import           TestHelpers

main :: IO ()
main = do
    hspec suite

suite :: Spec
suite = do
    describe "Requests" $ do
        it "have corresponding telemetric data" $ do
            runTestDaemon "tcp://localhost:1234" loadState
            sig     <- newEmptyMVar
            client  <- telemetry sig
            _       <- testWriter sig writeThings
            putMVar sig ()
            x       <- wait client
            x `shouldBe` [ WriterSimplePoints
                         , WriterExtendedPoints
                         , WriterRequest
                         , WriterRequestLatency
                         , WriterCephLatency ]


telemetry :: MVar () -> IO (Async [TeleMsgType])
telemetry quit = async $ do
    -- setup a broker for telemetry
    linkThread $ do
        Z.runZMQ $ startProxy (XPub,"tcp://*:6660")
                              (XSub,"tcp://*:6661")
                              "tcp://*:6000"

    -- connect to the broker for telemtrics
    withContext $ \ctx ->
      withSocket ctx Sub $ \sock -> do
        connect sock $ "tcp://localhost:6660"
        subscribe sock ""
        L.nub <$> L.sort <$> execStateT (go sock) []
    where go sock = do
            done <- isJust <$> liftIO (tryReadMVar quit)
            unless done $ do
              x <- liftIO $ receive sock
              case (fromWire x :: Either SomeException TeleResp) of
                Right y -> modify ((_type $ _msg y):)
                _       -> error "Unrecognised telemetric response"
              go sock

testWriter :: MVar () -> IO () -> IO (Async (), Async ())
testWriter quit act = do
    -- setup a broker so we can "send" to this testWriter daemon
    linkThread $ do
        Z.runZMQ $ startProxy (Router,"tcp://*:5560")
                              (Dealer,"tcp://*:5561")
                              "tcp://*:5000"

    -- start the testWriter daemon and its profiler
    (args, prof) <- daemonArgs (fromJust $ parseURI "tcp://localhost:5561")
                                Nothing "test" quit
                               (Just "writer-test") (Just (6661, 1000))
    w <- async $ startWriter args 0
    p <- async $ startProfiler prof

    -- perform the fake "send" actions
    _ <- act
    return (w,p)

writeThings :: IO ()
writeThings = forM_ ([0..100]::[Int]) $ const sendTestMsg
