{-# LANGUAGE OverloadedStrings #-}
import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.List.NonEmpty (fromList)
import           Data.Maybe
import           Network.URI
import           System.ZMQ4 hiding (shutdown)
import qualified System.ZMQ4.Monadic as Z
import qualified System.IO as IO

import           Vaultaire.Daemon
import           Vaultaire.Broker
import           Vaultaire.Writer
import           Vaultaire.Profiler
import           Vaultaire.Types
import           Vaultaire.Util
import           TestHelpers

{-
main :: IO ()
main = do
    now <- getCurrentTime
    hspec (suite now)

suite :: UTCTime -> Spec
suite now = do
    describe "Writer requests" $ do
        it "has corresponding telemetric data" $ do
            done <- newEmptyMVar
            runTelemetrySub
            runWriterThen $ forM_ [0..1000] $ const sendPonyMsg
            putMVar done ()
-}

main = do
  -- create the testing environment
  runTestDaemon "tcp://localhost:1234" loadState

  sig                <- newEmptyMVar
  client             <- telemetry sig
  (server, profiler) <- writer sig writeThings
  waitAny [client, server, profiler]

telemetry :: MVar () -> IO (Async ())
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
        go sock
    where go sock = do
            done <- isJust <$> liftIO (tryReadMVar quit)
            unless done $ do
              x <- receive sock
              case (fromWire x :: Either SomeException TeleResp) of
                Right y -> print $ show y
                _ -> error "huh"
              go sock

writer :: MVar () -> IO () -> IO (Async (), Async ())
writer quit act = do
    -- setup a broker so we can "send" to this writer daemon
    linkThread $ do
        Z.runZMQ $ startProxy (Router,"tcp://*:5560")
                              (Dealer,"tcp://*:5561")
                              "tcp://*:5000"

    -- start the writer daemon and its profiler
    (args, prof) <- daemonArgs (fromJust $ parseURI "tcp://localhost:5561")
                                Nothing "test" quit
                               (Just "writer-test") (Just (6661, 1000))
    writer   <- async $ startWriter args 0
    profiler <- async $ startProfiler prof

    -- perform the fake "send" actions
    r <- act
    return (writer, profiler)

  where handler (Message rep_f (Origin origin) msg ) =
            rep_f . PassThrough $ origin `B.append` msg

writeThings :: IO ()
writeThings = forM_ ([0..100]::[Int]) $ const sendTestMsg
