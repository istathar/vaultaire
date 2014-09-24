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
import Control.Concurrent.Async
import Data.Binary.IEEE754 (doubleToWord, wordToDouble)
import qualified Data.ByteString.Char8 as S
import Data.Word
import Options.Applicative hiding (Parser, option)
import Options.Applicative
import System.Log.Logger
import Text.Printf

import Marquise.Client
import Package
import Vaultaire.Program
import Vaultaire.Types

data Options = Options {
    debug :: Bool
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
    Options{..} <- execParser helpfulParser

    let level = if debug
        then Debug
        else Normal

    quit <- initializeProgram (package ++ "-" ++ version) level

    logM "Main.main" DEBUG "Starting generator"

    spool <- createSpoolFiles "demowave"

    let a = hashIdentifier "This is a test of the emergency broadcast system"
    loop quit spool a False

    logM "Main.main" DEBUG "End"


loop :: MVar () -> SpoolFiles -> Address -> Bool -> IO ()
loop _        _     _       True  = return ()
loop shutdown spool address False = do
--  done <- isJust <$> tryReadMVar shutdown
--  unless done $ do
    i <- getCurrentTimeNanoseconds
    let v = demoWaveAt i
    let msg = printf "%s\t%d\t% 9.6f" (show address) (unTimeStamp i) (wordToDouble v)
    logM "Main.loop" DEBUG msg
    queueSimple spool address i v
--      threadDelay (5 * 1000000)   -- every 5 s

    a1 <- async (do
        readMVar shutdown
        return True)

    a2 <- async (do
        threadDelay (5 * 1000000)   -- every 5 s
        return False)

    (_,done) <- waitAny [a1,a2]
    loop shutdown spool address done

demoWaveAt :: TimeStamp -> Word64
demoWaveAt (TimeStamp x) =
    let
        period = 3600 * 3
        f = 1/period                                    -- instances per second
        w = 2 * pi * f :: Double
        t = ((/ 1e9) . fromRational . toRational) x
        y = sin (w * t)
    in
        doubleToWord y

