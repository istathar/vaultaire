{-# LANGUAGE RecordWildCards #-}

module Main where
import Control.Exception (throw)
import Control.Monad
import qualified Data.ByteString.Char8 as S
import Data.Maybe (fromJust)
import Data.Packer (putWord64LE, runPacking)
import Data.Word (Word32, Word64)
import Marquise.Server (marquiseServer)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.Directory
import System.Log.Handler.Syslog
import System.Log.Logger
import System.Rados.Monadic (RadosError (..), runObject, stat, writeFull)
import System.ZMQ4.Monadic
import Text.Trifecta
import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.Daemon (extendedDayOID, simpleDayOID, withPool, dayMapsFromCeph)
import Vaultaire.Reader (startReader)
import Vaultaire.Types
import Vaultaire.Util
import Vaultaire.Writer (startWriter)
import Marquise.Client
import Data.String
import Pipes

data Options = Options
  { pool      :: String
  , user      :: String
  , broker    :: String
  , debug     :: Bool
  , component :: Component }

data Component = Broker
               | Reader
               | Writer { batchPeriod :: Word32, bucketSize :: Word64 }
               | Marquise { origin :: Origin, namespace :: String }
               | Contents
               | RegisterOrigin { origin :: Origin, buckets :: Word64 }
               | Read { origin  :: Origin
                      , address :: Address
                      , start   :: Word64
                      , end     :: Word64 }
               | List { origin :: Origin }
               | DumpDays { origin :: Origin }

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
       <> parseMarquiseComponent
       <> parseContentsComponent
       <> parseRegisterOriginComponent
       <> parseReadComponent
       <> parseListComponent
       <> parseDumpDaysComponent )

    parseBrokerComponent = command "broker" $
        info (pure Broker) (progDesc "Start a broker deamon")

    parseReaderComponent = command "reader" $
        info (pure Reader) (progDesc "Start a writer daemon")

    parseWriterComponent = command "writer" $
        info writerOptionsParser (progDesc "Start a writer daemon")

    parseMarquiseComponent = command "marquise" $
        info marquiseOptionsParser (progDesc "Start a marquise daemon")

    parseContentsComponent = command "contents" $
        info (pure Contents) (progDesc "Start a contents daemon")

    parseRegisterOriginComponent = command "register" $
        info registerOriginParser (progDesc "Register a new origin")

    parseReadComponent = command "read" $
        info readOptionsParser (progDesc "Read points")

    parseListComponent = command "list" $
        info listOptionsParser (progDesc "List addresses and metadata in origin")

    parseDumpDaysComponent = command "days" $
        info dumpDaysParser (progDesc "Display the current day map contents")


parseOrigin :: O.Parser Origin
parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")
  where
    mkOrigin = Origin . S.pack

readOptionsParser :: O.Parser Component
readOptionsParser = Read <$> parseOrigin
                         <*> parseAddress
                         <*> parseStart
                         <*> parseEnd
  where
    parseAddress = argument (fmap fromString . str) (metavar "ADDRESS")
    parseStart = O.option $
        long "start"
        <> short 's'
        <> value 0
        <> showDefault
        <> help "Start time in nanoseconds since epoch"
    parseEnd = O.option $
        long "end"
        <> short 'e'
        <> value maxBound
        <> showDefault
        <> help "End time in nanoseconds since epoch"

listOptionsParser :: O.Parser Component
listOptionsParser = List <$> parseOrigin

dumpDaysParser :: O.Parser Component
dumpDaysParser = DumpDays <$> parseOrigin

registerOriginParser :: O.Parser Component
registerOriginParser = RegisterOrigin <$> parseOrigin <*> parseBuckets
  where
    parseBuckets = O.option $
        long "buckets"
        <> short 'n'
        <> value 128
        <> showDefault
        <> help "Number of buckets to distribute writes over"


writerOptionsParser :: O.Parser Component
writerOptionsParser = Writer <$> parseBatchPeriod <*> parseBucketSize
  where
    parseBatchPeriod = O.option $
        long "batch_period"
        <> short 'p'
        <> value 4
        <> showDefault
        <> help "Number of seconds to wait before flushing writes"

    parseBucketSize = O.option $
        long "roll_over_size"
        <> short 'r'
        <> value 4194304
        <> showDefault
        <> help "Maximum bytes in any given bucket before rollover"

marquiseOptionsParser :: O.Parser Component
marquiseOptionsParser = Marquise <$> parseOrigin <*> parseNameSpace
  where
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
        Writer batch_period roll_over_size -> runWriter pool user broker batch_period roll_over_size
        Marquise origin namespace -> marquiseServer broker origin namespace
        Contents -> runContents pool user broker
        RegisterOrigin origin buckets -> runRegisterOrigin pool user origin buckets
        Read origin addr start end -> runRead broker origin addr start end
        List origin -> runList broker origin
        DumpDays origin -> runDumpDays pool user origin

runRead :: String -> Origin -> Address -> Word64 -> Word64 -> IO ()
runRead broker origin addr start end =
    withReaderConnection broker $ \c ->
        runEffect $ for (readSimple addr start end origin c >-> decodeSimple)
                        (lift . print)

runList :: String -> Origin -> IO ()
runList broker origin =
    withContentsConnection broker $ \c ->
        runEffect $ for (enumerateOrigin origin c) (lift . print)

runDumpDays :: String -> String -> Origin -> IO ()
runDumpDays pool user origin =  do
    let user' = Just (S.pack user)
    let pool' = S.pack pool

    maps <- withPool user' pool' (dayMapsFromCeph origin)
    case maps of
        Left e -> error e
        Right ((_, simple), (_, extended)) -> do
            putStrLn "Simple day map:"
            print simple
            putStrLn "Extended day map:"
            print extended


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
    waitForever

runReader :: String -> String -> String -> IO ()
runReader pool user broker =
    startReader ("tcp://" ++ broker ++ ":5571")
                (Just $ S.pack user)
                (S.pack pool)

runWriter :: String -> String -> String -> Word32 -> Word64 -> IO ()
runWriter pool user broker poll_period bucket_size =
    startWriter ("tcp://" ++ broker ++ ":5561")
                (Just $ S.pack user)
                (S.pack pool)
                bucket_size
                (fromIntegral poll_period)

runContents :: String -> String -> String -> IO ()
runContents pool user broker =
    startContents ("tcp://" ++ broker ++ ":5581")
                (Just $ S.pack user)
                (S.pack pool)

runRegisterOrigin :: String -> String -> Origin -> Word64 -> IO ()
runRegisterOrigin pool user origin buckets = do
    let targets = [simpleDayOID origin, extendedDayOID origin]
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
