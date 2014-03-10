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
formatTimestamp x = formatTime defaultTimeLocale "%H:%M:%S.%q" x


getTimestamp :: IO String
getTimestamp = do
    cur <- getCurrentTime
    let t = formatTimestamp cur
    let n  = S.length "07:12:21.999"
    let s = take n t
    return $ s ++ "Z"


program :: Options -> IO ()
program (Options broker fields) = do
    runZMQ $ do
        tele <- socket Sub
        connect tele  ("tcp://" ++ broker ++ ":5580")
        forM_ fields (\field -> do
            subscribe tele (S.pack field))

        loop tele 13 9 8
  where
        loop tele iW0 hW0 kW0 =  do
            msg <- receiveMulti tele
            let [k, v, u, i, h1] = map S.unpack msg
            let h = takeWhile (/= '.') h1

            t <- liftIO $ getTimestamp

            let hW = if length h > hW0 then length h else hW0
            let iW = if length i > iW0 then length i else iW0
            let kW = if (length k + 1) > kW0 then length k + 1 else kW0

            liftIO $ putStrLn $ printf "%s %-*s %-*s %-*s %s %s" t iW i hW h kW (k ++ ":") (align v) u

            loop tele iW hW kW


align :: String -> String
align v =
  let
    (integral,fraction) = span (/= '.') v
  in
    printf "%6s%-4s" integral fraction -- three digits, plus the decimal


toplevel :: Parser Options
toplevel = Options
    <$> argument str
            (metavar "BROKER" <>
             help "Host name or IP address of broker to read cluster telemetry from")
    <*> (some (argument str
            (metavar "FIELDS" <>
             help "Fields you wish to subscribe to (if unspecified then all fields)"))
                <|> pure [""])


commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Simple utility to read telemetry from a vaultaire cluster" <>
                header "A data vault for metrics")
