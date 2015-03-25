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

{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import Control.Monad
import Data.Maybe (fromJust)
import Data.Word (Word64)
import Network.BSD (getHostName)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.Directory
import System.Environment
import System.Log.Logger
import Text.Read
import Text.Trifecta

import DaemonRunners
import Package (package, version)
import Vaultaire.Program
import Vaultaire.Types (agentIDLength)

data Options = Options
  { pool      :: String
  , user      :: String
  , broker    :: String
  , debug     :: Bool
  , quiet     :: Bool
  , noprofile :: Bool
  , period    :: Int
  , bound     :: Int
  , name      :: String
  , keyring   :: String
  , component :: Component }

data Component = Broker
               | Reader
               | Writer { bucketSize :: Word64 }
               | Contents

-- | Command line option parsing

helpfulParser :: Options -> O.ParserInfo Options
helpfulParser os = info (helper <*> optionsParser os) fullDesc

optionsParser :: Options -> O.Parser Options
optionsParser Options{..} = Options <$> parsePool
                                    <*> parseUser
                                    <*> parseBroker
                                    <*> parseDebug
                                    <*> parseQuiet
                                    <*> parseNoprofile
                                    <*> parsePeriod
                                    <*> parseBound
                                    <*> parseName
                                    <*> parseKeyring
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
        <> help "Output lots of debugging information"

    parseQuiet = switch $
           long "quiet"
        <> short 'q'
        <> help "Only emit warnings or fatal messages"

    parseNoprofile = switch $
           long "no-profiling"
        <> help "Disables profiling"

    parsePeriod = O.option auto $
           long "period"
        <> metavar "PERIOD"
        <> value period
        <> showDefault
        <> help "How often the profiler reports telemetric data, in milliseconds."

    parseBound = O.option auto $
           long "bound"
        <> metavar "BOUND"
        <> value bound
        <> showDefault
        <> help "How many stat reports the profiler can handle per period before it starts losing accuracy."

    parseName = strOption $
           long "name"
        <> short 'n'
        <> metavar "NAME"
        <> value name
        <> showDefault
        <> help "Identifiable name for the daemon used for telemetry messages."

    parseKeyring = strOption $
           long "keyring"
        <> short 'k'
        <> metavar "KEYRING"
        <> value ""
        <> help "Path to Ceph keyring file. If set, this will override the CEPH_KEYRING environment variable."

    parseComponents = subparser
       (  parseBrokerComponent
       <> parseReaderComponent
       <> parseWriterComponent
       <> parseContentsComponent )

    parseBrokerComponent =
        componentHelper "broker" (pure Broker) "Start a broker daemon"

    parseReaderComponent =
        componentHelper "reader" (pure Reader) "Start a reader daemon"

    parseWriterComponent =
        componentHelper "writer" writerOptionsParser "Start a writer daemon"

    parseContentsComponent =
        componentHelper "contents" (pure Contents) "Start a contents daemon"

    componentHelper cmd_name parser desc =
        command cmd_name (info (helper <*> parser) (progDesc desc))


writerOptionsParser :: O.Parser Component
writerOptionsParser = Writer <$> parseBucketSize
  where
    parseBucketSize = O.option auto $
        long "roll_over_size"
        <> short 'r'
        <> value 4194304
        <> showDefault
        <> help "Maximum bytes in any given bucket before rollover"

-- | Config file parsing
parseConfig :: FilePath -> IO Options
parseConfig fp = do
    exists <- doesFileExist fp
    defaults <- liftM defaultConfig defaultAgentID
    if exists
        then do
            maybe_ls <- parseFromFile configParser fp
            case maybe_ls of
                Just ls -> return $ mergeConfig ls defaults
                Nothing  -> error "Failed to parse config"
        else return defaults
  where
    defaultConfig host =
        Options "vaultaire" -- Use 'vaultaire' rados pool.
                "vaultaire" -- Connect as 'vaultaire' user.
                "localhost" -- Default to broker on localhost.
                False       -- Don't print debug output.
                False       -- Don't suppress all output.
                False       -- Don't disable profiling.
                1000        -- Write telemetry every 1000ms.
                2048        -- Handle <= 2048 telemetry reports per period.
                host        -- Use our hostname for telemetry agent ID.
                ""          -- Don't set CEPH_KEYRING.
                Broker      -- Run in broker mode.

    mergeConfig ls Options{..} = fromJust $
        Options <$> lookup "pool" ls `mplus` pure pool
                <*> lookup "user" ls `mplus` pure user
                <*> lookup "broker" ls `mplus` pure broker
                <*> pure debug
                <*> pure quiet
                <*> pure noprofile
                <*> join (readMaybe <$> lookup "period" ls) `mplus` pure period
                <*> join (readMaybe <$> lookup "bound"  ls) `mplus` pure period
                <*> lookup "name" ls `mplus` pure name
                <*> lookup "keyring" ls `mplus` pure keyring
                <*> pure Broker

    -- | Construct a sensible default agent ID - the machine's hostname,
    --   truncated to the max agent ID length.
    defaultAgentID = fmap (take agentIDLength) getHostName

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

-- If set, override CEPH_KEYRING envvar (used by librados).
updateCephKeyring :: String -> IO ()
updateCephKeyring "" = return ()
updateCephKeyring k  = setEnv "CEPH_KEYRING" k

--
-- Main program entry point
--

main :: IO ()
main = do
    Options{..} <- parseArgsWithConfig "/etc/vaultaire.conf"

    let level | debug     = Debug
              | quiet     = Quiet
              | otherwise = Normal

    updateCephKeyring keyring

    quit <- initializeProgram (package ++ "-" ++ version) level

    -- Run daemon(s, at present just one). These are all expected to fork
    -- threads and return the Async representing them. If they wish to
    -- requeust termination they have to put unit into the shutdown MVar and
    -- then return; they need to finish up and return if something else puts
    -- unit into the MVar.

    debugM "Main.main" "Starting component"

    a <- case component of
        Broker ->
            runBrokerDaemon quit

        Reader ->
            if   noprofile
            then runReaderDaemon pool user broker quit name Nothing
            else runReaderDaemon pool user broker quit name (Just (period,bound))

        Writer roll_over_size ->
            if   noprofile
            then runWriterDaemon pool user broker roll_over_size quit name Nothing
            else runWriterDaemon pool user broker roll_over_size quit name (Just (period,bound))

        Contents ->
            if   noprofile
            then runContentsDaemon pool user broker quit name Nothing
            else runContentsDaemon pool user broker quit name (Just (period,bound))

    -- Block until shutdown triggered
    debugM "Main.main" "Running until shutdown"
    _ <- waitDaemon a
    debugM "Main.main" "End"
