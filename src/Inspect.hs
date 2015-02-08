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

import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Monad
import qualified Data.ByteString.Char8 as S
import Data.Maybe (fromJust)
import Data.String
import Data.Word (Word64)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import System.Directory
import System.Log.Logger
import Text.Trifecta

import CommandRunners
import DaemonRunners (forkThread)
import Marquise.Client
import Package (package, version)
import Vaultaire.Program


data Options = Options
  { pool      :: String
  , user      :: String
  , broker    :: String
  , debug     :: Bool
  , quiet     :: Bool
  , component :: Component }

data Component =
                 None
               | RegisterOrigin { origin  :: Origin
                                , buckets :: Word64
                                , step    :: Word64
                                , start   :: TimeStamp
                                , end     :: TimeStamp }
               | Read { origin  :: Origin
                      , address :: Address
                      , start   :: TimeStamp
                      , end     :: TimeStamp }
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
                                    <*> parseQuiet
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

    parseComponents = subparser
       (  parseRegisterOriginComponent
       <> parseReadComponent
       <> parseListComponent
       <> parseDumpDaysComponent )

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
parseOrigin = argument (fmap mkOrigin str) (metavar "ORIGIN")
  where
    mkOrigin = Origin . S.pack

readOptionsParser :: O.Parser Component
readOptionsParser = Read <$> parseOrigin
                         <*> parseAddress
                         <*> parseStart
                         <*> parseEnd
  where
    parseAddress = argument (fmap fromString str) (metavar "ADDRESS")
    parseStart = O.option auto $
        long "start"
        <> short 's'
        <> value 0
        <> showDefault
        <> help "Start time in nanoseconds since epoch"

    parseEnd = O.option auto $
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
    parseBuckets = O.option auto $
        long "buckets"
        <> short 'n'
        <> value 128
        <> showDefault
        <> help "Number of buckets to distribute writes over"

    parseStep = O.option auto $
        long "step"
        <> short 's'
        <> value 14400000000000
        <> showDefault
        <> help "Back-dated rollover period (see documentation: TODO)"

    parseBegin = O.option auto $
        long "begin"
        <> short 'b'
        <> value 0
        <> showDefault
        <> help "Back-date begin time (default is no backdating)"

    parseEnd = O.option auto $
        long "end"
        <> short 'e'
        <> value 0
        <> showDefault
        <> help "Back-date end time"

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
    defaultConfig = Options "vaultaire" "vaultaire" "localhost" False False None
    mergeConfig ls Options{..} = fromJust $
        Options <$> lookup "pool" ls `mplus` pure pool
                <*> lookup "user" ls `mplus` pure user
                <*> lookup "broker" ls `mplus` pure broker
                <*> pure debug
                <*> pure quiet
                <*> pure None

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

--
-- Main program entry point
--

main :: IO ()
main = do
    Options{..} <- parseArgsWithConfig "/etc/vaultaire.conf"

    level
        | debug     = Debug
        | quiet     = Quiet
        | otherwise = Normal

    quit <- initializeProgram (package ++ "-" ++ version) level

    -- Run selected command.
    debugM "Main.main" "Starting command"

    -- Although none of the commands are running in the background, we get off
    -- of the main thread so that we can block the main thread on the quit
    -- semaphore, such that a user interrupt will kill the program.

    a <- forkThread $ do
        case component of
            None -> return ()
            RegisterOrigin origin buckets step begin end ->
                runRegisterOrigin pool user origin buckets step begin end
            Read _ _ _ _ ->
                error "Currently unimplemented. Use marquise's `data read` command"
            List _ ->
                error "Currently unimplemented. Use marquise's `data list` command"
            DumpDays origin ->
                runDumpDayMap pool user origin
        putMVar quit ()

    wait a
    debugM "Main.main" "End"

