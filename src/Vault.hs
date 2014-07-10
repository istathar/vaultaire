--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE RecordWildCards #-}

module Main where
import Control.Concurrent
import Control.Exception (throw)
import Control.Monad
import qualified Data.ByteString.Char8 as S
import Data.Map (fromAscList)
import Data.Maybe (fromJust)
import Data.String
import Data.Word (Word32, Word64)
import GHC.Conc
import Marquise.Client
import Marquise.Server (marquiseServer)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import Pipes
import System.Directory
import System.Log.Handler.Syslog
import System.Log.Logger
import System.Rados.Monadic (RadosError (..), runObject, stat, writeFull)
import System.ZMQ4.Monadic
import Text.Trifecta
import Vaultaire.Broker
import Vaultaire.ContentsServer
import Vaultaire.Daemon (dayMapsFromCeph, extendedDayOID, simpleDayOID,
                         withPool)
import Vaultaire.Reader (startReader)
import Vaultaire.Types
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
               | Writer { batchPeriod :: Word32, bucketSize :: Word64 }
               | Marquise { origin :: Origin, namespace :: String }
               | Contents
               | RegisterOrigin { origin  :: Origin
                                , buckets :: Word64
                                , step    :: Word64
                                , begin   :: Word64
                                , end     :: Word64 }
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

    parseBrokerComponent =
        componentHelper "broker" (pure Broker) "Start a broker daemon"

    parseReaderComponent =
        componentHelper "reader" (pure Reader) "Start a reader daemon"

    parseWriterComponent =
        componentHelper "writer" writerOptionsParser "Start a writer daemon"

    parseMarquiseComponent =
        componentHelper "marquise" marquiseOptionsParser "Start a marquise daemon"

    parseContentsComponent =
        componentHelper "contents" (pure Contents) "Start a contents daemon"

    parseRegisterOriginComponent =
        componentHelper "register" registerOriginParser "Register a new origin"

    parseReadComponent =
        componentHelper "read" readOptionsParser "Read points"

    parseListComponent =
        componentHelper "list" listOptionsParser "List addresses and metadata in origin"

    parseDumpDaysComponent =
        componentHelper "days" dumpDaysParser "Display the current day map contents"

    componentHelper cmd_name parser desc =
        command cmd_name (info (helper <*> parser) (progDesc desc))

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
registerOriginParser = RegisterOrigin <$> parseOrigin
                                      <*> parseBuckets
                                      <*> parseStep
                                      <*> parseBegin
                                      <*> parseEnd
  where
    parseBuckets = O.option $
        long "buckets"
        <> short 'n'
        <> value 128
        <> showDefault
        <> help "Number of buckets to distribute writes over"

    parseStep = O.option $
        long "step"
        <> short 's'
        <> value 14400000000000
        <> showDefault
        <> help "Back-dated rollover period (see documentation: TODO)"

    parseBegin = O.option $
        long "begin"
        <> short 'b'
        <> value 0
        <> showDefault
        <> help "Back-date begin time (default is no backdating)"

    parseEnd = O.option $
        long "end"
        <> short 'e'
        <> value 0
        <> showDefault
        <> help "Back-date end time"

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
    -- command line +RTS -Nn -RTS value
    when (numCapabilities == 1) (getNumProcessors >>= setNumCapabilities)

    Options{..} <- parseArgsWithConfig "/etc/vaultaire.conf"

    -- Start and configure logger
    let log_level = if debug then DEBUG else WARNING
    logger <- openlog "vaultaire" [PID] USER log_level
    updateGlobalLogger rootLoggerName (addHandler logger . setLevel log_level)

    -- Shutdown is signaled by putting a () into the MVar
    --
    -- TODO: afcowie: add signal handler for this
    shutdown <- newEmptyMVar

    debugM "Main.main" "Logger initialized, starting component"

    case component of
        Broker ->
            runBroker shutdown
        Reader ->
            runReader pool user broker shutdown
        Writer batch_period roll_over_size ->
            runWriter pool user broker batch_period roll_over_size shutdown
        Marquise origin namespace ->
            marquiseServer broker origin namespace
        Contents ->
            runContents pool user broker shutdown
        RegisterOrigin origin buckets step begin end ->
            runRegisterOrigin pool user origin buckets step begin end
        Read origin addr start end ->
            runRead broker origin addr start end
        List origin ->
            runList broker origin
        DumpDays origin ->
            runDumpDays pool user origin

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


runBroker :: MVar () -> IO ()
runBroker shutdown = runZMQ $ do
    -- Writer proxy.
    void $ async $ startProxy (Router,"tcp://*:5560")
                              (Dealer,"tcp://*:5561")
                              "tcp://*:5000"

    -- Reader proxy.
    void $ async $ startProxy (Router,"tcp://*:5570")
                              (Dealer,"tcp://*:5571")
                              "tcp://*:5001"

    -- Contents proxy.
    void $ async $ startProxy (Router,"tcp://*:5580")
                              (Dealer,"tcp://*:5581")
                              "tcp://*:5002"

    liftIO $ do
        debugM "Main.runBroker" "Proxies started."
        readMVar shutdown

runReader :: String -> String -> String -> MVar () -> IO ()
runReader pool user broker =
    startReader ("tcp://" ++ broker ++ ":5571")
                (Just $ S.pack user)
                (S.pack pool)

runWriter :: String -> String -> String -> Word32 -> Word64 -> MVar () -> IO ()
runWriter pool user broker poll_period bucket_size =
    startWriter ("tcp://" ++ broker ++ ":5561")
                (Just $ S.pack user)
                (S.pack pool)
                bucket_size
                (fromIntegral poll_period)

runContents :: String -> String -> String -> MVar () -> IO ()
runContents pool user broker =
    startContents ("tcp://" ++ broker ++ ":5581")
                (Just $ S.pack user)
                (S.pack pool)

runRegisterOrigin :: String -> String -> Origin -> Word64 -> Word64 -> Word64 -> Word64 -> IO ()
runRegisterOrigin pool user origin buckets step begin end = do
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

            writeFull (toWire dayMap) >>= maybe (return ()) throw

    dayMap = DayMap . fromAscList $
        ((0, buckets):)[(n, buckets) | n <- [begin,begin+step..end]]
