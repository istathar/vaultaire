{-# LANGUAGE RecordWildCards #-}

module Main where
import Control.Monad
import qualified Data.ByteString.Char8 as BS
import Data.Maybe (fromJust)
import Data.Word (Word32)
import Marquise.Server (marquiseServer)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.Directory
import System.Log.Handler.Syslog
import System.Log.Logger
import System.ZMQ4.Monadic
import Text.Trifecta
import Vaultaire.Broker
import Vaultaire.Reader (startReader)
import Vaultaire.Util
import Vaultaire.Writer (startWriter)

data Options = Options
  { pool      :: String
  , user      :: String
  , broker    :: String
  , debug     :: Bool
  , component :: Component }

data Component = Broker
               | Reader
               | Writer { batchPeriod :: Word32 }
               | Marquise { origin :: String, namespace :: String }

-- | Command line option parsing

helpfulParser :: Options -> O.ParserInfo Options
helpfulParser os = info (helper <*> optionsParser os) fullDesc

optionsParser :: Options -> O.Parser Options
optionsParser Options{..} = Options <$> parsePool
                                    <*> parseUser
                                    <*> parseBroker
                                    <*> parseDebug
                                    <*> parseComponents
  where
    parsePool = strOption $
           long "pool"
        <> short 'p'
        <> metavar "POOL"
        <> value pool
        <> showDefault
        <> help "Ceph pool name for storage"

    parseUser = strOption $
           long "user"
        <> short 'u'
        <> metavar "USER"
        <> value user
        <> showDefault
        <> help "Ceph user for access to storage"

    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value broker
        <> showDefault
        <> help "Vault broker host name or IP address"

    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Set log level to DEBUG"

    parseComponents = subparser
       (   parseBrokerComponent
       <> parseReaderComponent
       <> parseWriterComponent
       <> parseMarquiseComponent )

    parseBrokerComponent = command "broker" $
        info (pure Broker) (progDesc "Start a broker deamon")

    parseReaderComponent = command "reader" $
        info (pure Reader) (progDesc "Start a writer daemon")

    parseWriterComponent = command "writer" $
        info writerOptionsParser (progDesc "Start a writer daemon")

    parseMarquiseComponent = command "marquise" $
        info marquiseOptionsParser (progDesc "Start a marquise daemon")

writerOptionsParser :: O.Parser Component
writerOptionsParser = Writer <$> O.option (
       long "batch_period"
    <> short 'p'
    <> value 4
    <> showDefault
    <> help "Number of seconds to wait before flushing writes" )

marquiseOptionsParser :: O.Parser Component
marquiseOptionsParser = Marquise <$> parseOrigin <*> parseNameSpace
  where
    parseOrigin = strOption $
        long "origin"
        <> short 'o'
        <> metavar "ORIGIN"
        <> help "Origin to write to"
    parseNameSpace = strOption $
        long "namespace"
        <> short 'n'
        <> metavar "NAMESPACE"
        <> help "NameSpace to look for data in"

-- | Config file parsing

parseConfig :: FilePath -> IO Options
parseConfig fp = do
    exists <- doesFileExist fp
    if exists
        then do
            maybe_ls <- parseFromFile configParser fp
            case maybe_ls of
                Just ls -> return $ mergeConfig ls defaultConfig
                Nothing  -> error "Failed to parse config"
        else return defaultConfig
  where
    defaultConfig = Options "vaultaire" "vaultaire" "localhost" False Broker
    mergeConfig ls Options{..} = fromJust $
        Options <$> lookup "pool" ls `mplus` pure pool
                <*> lookup "user" ls `mplus` pure user
                <*> lookup "broker" ls `mplus` pure broker
                <*> pure debug
                <*> pure Broker

configParser :: Parser [(String, String)]
configParser = some $ liftA2 (,)
    (spaces *> possibleKeys <* spaces <* char '=')
    (spaces *> (stringLiteral <|> stringLiteral'))

possibleKeys :: Parser String
possibleKeys =
        string "pool"
    <|> string "user"
    <|> string "broker"

parseArgsWithConfig :: FilePath -> IO Options
parseArgsWithConfig = parseConfig >=> execParser . helpfulParser

main :: IO ()
main = do
    Options{..} <- parseArgsWithConfig "/etc/vaultaire.conf"

    let log_level = if debug then DEBUG else WARNING
    logger <- openlog "vaultaire" [PID] USER log_level
    updateGlobalLogger rootLoggerName (addHandler logger . setLevel log_level)

    debugM "Main.main" "Logger initialized, starting component"

    case component of
        Broker -> runBroker
        Reader -> runReader pool user broker
        Writer batch_period -> runWriter pool user broker batch_period
        Marquise origin namespace -> marquiseServer broker origin namespace

runBroker :: IO ()
runBroker = runZMQ $ do
    void $ async $ startProxy (Router,"tcp://*:5560")
                              (Dealer,"tcp://*:5561")
                              "tcp://*:5000"

    void $ async $ startProxy (Router,"tcp://*:5570")
                              (Dealer,"tcp://*:5571")
                              "tcp://*:5001"

    liftIO $ debugM "Main.runBroker" "Proxies started."
    waitForever

runReader :: String -> String -> String -> IO ()
runReader pool user broker =
    startReader ("tcp://" ++ broker ++ ":5561")
                (Just $ BS.pack user)
                (BS.pack pool)

runWriter :: String -> String -> String -> Word32 -> IO ()
runWriter pool user broker poll_period =
    startWriter ("tcp://" ++ broker ++ ":5571")
                (Just $ BS.pack user)
                (BS.pack pool)
                (fromIntegral poll_period)
