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

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where
import Control.Concurrent
import Control.Monad
import Data.Binary.IEEE754 (doubleToWord)
import qualified Data.ByteString.Char8 as S
import Data.Time.Clock.POSIX
import Data.Word
import GHC.Conc
import Marquise.Client
import Options.Applicative hiding (Parser, option)
import Options.Applicative
import System.Log.Handler.Syslog
import System.Log.Logger

data Options = Options {
    debug  :: Bool
} 

-- | Command line option parsing

helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser =
    Options <$> parseDebug
  where
    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Set log level to DEBUG"

parseOrigin :: Parser Origin
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

    logM "Main.main" DEBUG "Starting"

    spool <- createSpoolFiles "demowave"

    let a = hashIdentifier "This is a test of the emergency broadcast system"

    forever $ do
        i <- getCurrentTimeNanoseconds
        let v = demoWaveAt i
        logM "Main.loop" DEBUG (show a ++ "\t" ++ show i ++ "\t" ++ show v)
        queueSimple spool a i v
        threadDelay (5 * 1000000)

demoWaveAt :: TimeStamp -> Word64
demoWaveAt (TimeStamp x) =
    let
        period = 3600 * 3
        f = 1/period                                    -- instances per second
        w = 2 * pi * f :: Double
        y t = sin (w * ((fromRational . toRational) t))
    in
        doubleToWord (y x)

