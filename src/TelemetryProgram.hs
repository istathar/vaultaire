--
-- Data vault for metrics
--
-- Copyright © 2011-2013 Operational Dynamics Consuting Pty Ltd
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PackageImports     #-}
{-# LANGUAGE RecordWildCards    #-}

module TelemetryProgram where

import Control.Monad
import Control.Monad.IO.Class
import qualified Data.ByteString.Char8 as S
import Data.Time.Clock (UTCTime, getCurrentTime)
import Data.Time.Format (formatTime)
import Options.Applicative
import System.Locale (defaultTimeLocale)
import System.ZMQ4.Monadic hiding (source)
import Text.Printf


data Options = Options {
    argDaemonHost :: !String,
    argFields     :: !([String])
}


formatTimestamp :: UTCTime -> String
formatTimestamp x = formatTime defaultTimeLocale "%a %e %b %y, %H:%M:%S.%q" x


getTimestamp :: IO String
getTimestamp = do
    cur <- getCurrentTime
    let t = formatTimestamp cur
    let n  = S.length "Sat  8 Oct 11, 07:12:21.999"
    let s = take n t
    return $ s ++ "Z"


program :: Options -> IO ()
program (Options daemon fields) = do
    runZMQ $ do
        telem <- socket Sub
        connect telem  ("tcp://" ++ daemon ++ ":5569")
        forM_ fields (\field -> do
            subscribe telem (S.pack field))

        forever $ do
            [k',v',u'] <- receiveMulti telem
            let k = S.unpack k'
            let v = S.unpack v'
            let u = S.unpack u'

            t <- liftIO $ getTimestamp

            liftIO $ putStrLn $ printf "%s  %-10s %-9s  %s" t (k ++ ":") v u


toplevel :: Parser Options
toplevel = Options
    <$> argument str
            (metavar "DAEMON" <>
             help "Host name or IP address of ingestd to follow")
    <*> (some (argument str
            (metavar "FIELDS" <>
             help "Fields you wish to subscribe to (if unspecified then all fields)"))
                <|> pure [""])


commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Simple utility to read telemetry from an ingestd" <>
                header "A data vault for metrics")
