{-# LANGUAGE RecordWildCards #-}

module Main where
import qualified Control.Concurrent.Async as A
import Control.Exception (throw)
import Control.Monad
import qualified Data.ByteString.Char8 as S
import Data.Packer (putWord64LE, runPacking)
import Data.Word (Word32, Word64)
import Marquise.Server (marquiseServer)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.Log.Handler.Syslog
import System.Log.Logger
import System.Rados.Monadic (RadosError (..), runObject, stat, writeFull)
import System.ZMQ4.Monadic
import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.Daemon (extendedDayOID, simpleDayOID, withPool)
import Vaultaire.Reader (startReader)
import Vaultaire.Types (Origin (..))
import Vaultaire.Writer (startWriter)

data Options = Options
  { pool      :: String
  , user      :: String
  , broker    :: String
  , debug     :: Bool
  , batchPeriod :: Word32
  , bucketSize :: Word64
  , origin :: String
  , namespace :: String
  , numBuckets :: Word64 }

-- | Command line option parsing

helpfulParser :: O.ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: O.Parser Options
optionsParser = Options <$> parsePool
                                    <*> parseUser
                                    <*> parseBroker
                                    <*> parseDebug
                                    <*> parseBatchPeriod
                                    <*> parseBucketSize
                                    <*> parseOrigin
                                    <*> parseNamespace
                                    <*> parseNumBuckets
  where
    parsePool = strOption $
           long "pool"
        <> short 'p'
        <> metavar "POOL"
        <> value "vaultaire-export"
        <> showDefault
        <> help "Ceph pool name for storage"

    parseUser = strOption $
           long "user"
        <> short 'u'
        <> metavar "USER"
        <> value "vaultaire"
        <> showDefault
        <> help "Ceph user for access to storage"

    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value "localhost"
        <> showDefault
        <> help "Vault broker host name or IP address"

    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Set log level to DEBUG"

    parseBatchPeriod = O.option $
        long "batch_period"
        <> short 'i'
        <> value 4
        <> showDefault
        <> help "Number of seconds to wait before flushing writes"

    parseBucketSize = O.option $
        long "roll_over_size"
        <> short 'r'
        <> value 4194304
        <> showDefault
        <> help "Maximum bytes in any given bucket before rollover"

    parseOrigin = strOption $
        long "origin"
        <> short 'o'
        <> metavar "ORIGIN"
        <> help "Origin to write to"

    parseNamespace = strOption $
        long "namespace"
        <> short 'n'
        <> metavar "NAMESPACE"
        <> help "Namespace to look for data in"

    parseNumBuckets = O.option $
        long "batch_period"
        <> short 'm'
        <> value 128
        <> showDefault
        <> help "Number of buckets"


main :: IO ()
main = do
    Options{..} <- execParser helpfulParser

    let log_level = if debug then DEBUG else WARNING
    logger <- openlog "vaultaire" [PID] USER log_level
    updateGlobalLogger rootLoggerName (addHandler logger . setLevel log_level)

    debugM "Main.main" "Logger initialized, starting components"

    runBroker
--  runRegisterOrigin pool user origin numBuckets
    runContents pool user broker
    runWriter pool user broker batchPeriod bucketSize
    runReader pool user broker
    marquiseServer broker origin namespace

runBroker :: IO ()
runBroker = runZMQ $ do
    void $ async $ startProxy (Router,"tcp://*:5560")
                              (Dealer,"tcp://*:5561")
                              "tcp://*:5000"

    void $ async $ startProxy (Router,"tcp://*:5570")
                              (Dealer,"tcp://*:5571")
                              "tcp://*:5001"

    void $ async $ startProxy (Router,"tcp://*:5580")
                              (Dealer,"tcp://*:5581")
                              "tcp://*:5002"

    liftIO $ debugM "Main.runBroker" "Proxies started."

runReader :: String -> String -> String -> IO ()
runReader pool user broker =
    void $ A.async $ startReader ("tcp://" ++ broker ++ ":5571")
                (Just $ S.pack user)
                (S.pack pool)

runWriter :: String -> String -> String -> Word32 -> Word64 -> IO ()
runWriter pool user broker poll_period bucket_size =
    void $ A.async $ startWriter ("tcp://" ++ broker ++ ":5561")
                (Just $ S.pack user)
                (S.pack pool)
                bucket_size
                (fromIntegral poll_period)

runContents :: String -> String -> String -> IO ()
runContents pool user broker =
    void $ A.async $ startContents ("tcp://" ++ broker ++ ":5581")
                (Just $ S.pack user)
                (S.pack pool)

runRegisterOrigin :: String -> String -> String -> Word64 -> IO ()
runRegisterOrigin pool user origin buckets = do
    let origin' = Origin (S.pack origin)
    let targets = [simpleDayOID origin', extendedDayOID origin']
    let user' = Just (S.pack user)
    let pool' = S.pack pool

    withPool user' pool' (forM_ targets initializeDayMap)
  where
    initializeDayMap target =
        runObject target $ do
            result <- stat
            case result of
                Left NoEntity{} -> return ()
                Left e -> throw e
                Right _ -> error "Origin already registered."

            writeFull contents >>= maybe (return ()) throw

    contents = runPacking 16 $ do
        putWord64LE 0
        putWord64LE buckets
