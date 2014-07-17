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
import Control.Monad
import qualified Data.ByteString.Char8 as S
import GHC.Conc
import Marquise.Client
import Marquise.Server (marquiseServer)
import Options.Applicative hiding (Parser, option)
import qualified Options.Applicative as O
import Pipes
import System.Log.Handler.Syslog
import System.Log.Logger
import Vaultaire.Types

data Options = Options
  { pool      :: String
  , user      :: String
  , broker    :: String
  , debug     :: Bool
  }

-- | Command line option parsing

helpfulParser :: O.ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: O.Parser Options
optionsParser = Options <$> parsePool
                                    <*> parseUser
                                    <*> parseBroker
                                    <*> parseDebug
  where
    parsePool = strOption $
           long "pool"
        <> short 'p'
        <> metavar "POOL"
        <> value "vaultaire"
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

parseOrigin :: O.Parser Origin
parseOrigin = argument (fmap mkOrigin . str) (metavar "ORIGIN")
  where
    mkOrigin = Origin . S.pack


main :: IO ()
main = do
    -- command line +RTS -Nn -RTS value
    when (numCapabilities == 1) (getNumProcessors >>= setNumCapabilities)

    Options{..} <- execParser helpfulParser

    -- Start and configure logger
    let log_level = if debug then DEBUG else WARNING
    logger <- openlog "vaultaire" [PID] USER log_level
    updateGlobalLogger rootLoggerName (addHandler logger . setLevel log_level)

    -- Shutdown is signaled by putting a () into the MVar
    --
    -- TODO: afcowie: add signal handler for this
    shutdown <- newEmptyMVar

    debugM "Main.main" "Starting"



    liftIO $ do
        debugM "Main.runBroker" "Proxies started."
        readMVar shutdown
