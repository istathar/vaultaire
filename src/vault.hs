--
-- Data vault for metrics
--
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
{-# OPTIONS -fno-warn-unused-imports #-}

module Main where

import Codec.Compression.LZ4
import Control.Applicative
import qualified Data.ByteString as S
import Options.Applicative

import Vaultaire.Conversion.Receiver


data Options = Options {
    optGlobalQuiet :: Bool,
    optCommand :: Command
}

data Command
    = Read ReadOptions
    | Contents ContentsOptions

data ReadOptions = ReadOptions {
    optReadOrigin :: String,
    optReadSource :: String
}

data ContentsOptions = ContentsOptions {
    optContentsOrigin :: String
}


toplevel :: Parser Options
toplevel = Options
    <$> switch
            (long "verbose" <>
             short 'v' <>
             help "Whether to be quiet")
    <*> subparser
            (command "read" (Read <$> readParser){- <>
             command "contents" (info contentsOptions
                    (progDesc "Get the contents list for this origin"))-})


readParser :: ParserInfo ReadOptions
readParser =
    info (helper <*> readOptions) (progDesc "Read values from a bucket")


readOptions :: Parser ReadOptions
readOptions =
    ReadOptions
    <$> argument str
            (metavar "ORIGIN")
    <*> argument str
            (metavar "SOURCE")
             


contentsOptions :: Parser Command
contentsOptions = undefined
{-
   ContentsOptions
    <$> argument str
            (metavar "ORIGIN")
-}



greet :: Options -> IO ()
greet (Options False _) = putStrLn "Hello"
greet _ = return ()


main = execParser commandLineParser >>= greet

commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc
                <> progDesc "Operations on a Vaultaire data vault."
                <> header "A data vault for metrics"
                <> footer txt)
  where
    txt =   "There is specific help available for each command;\n" ++
            "use vault COMMAND --help for details."
