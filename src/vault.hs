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
    optCommand     :: Command
}

data Command
    = Read ReadOptions
    | Contents ContentsOptions

data ReadOptions = ReadOptions {
    argReadOrigin :: String,
    argReadSource :: String,
    argReadTimestamp :: String
}

data ContentsOptions = ContentsOptions {
    argContentsOrigin :: String
}


toplevel :: Parser Options
toplevel = Options
    <$> switch
            (long "verbose" <>
             short 'v' <>
             help "Include diagnostic information in output")
    <*> subparser
            (command "read" (Read <$> readParser) <>
             command "contents" (Contents <$> contentsParser))


readParser :: ParserInfo ReadOptions
readParser =
    info (helper <*> readOptions) (progDesc "Dump the values in the bucket containing TIME")


readOptions :: Parser ReadOptions
readOptions =
    ReadOptions
    <$> argument str
            (metavar "ORIGIN")
    <*> argument str
            (metavar "SOURCE")
    <*> argument str
            (metavar "TIME")


contentsParser :: ParserInfo ContentsOptions
contentsParser =
    info (helper <*> contentsOptions) (progDesc "Get the contents list for this origin")


contentsOptions :: Parser ContentsOptions
contentsOptions =
    ContentsOptions
    <$> argument str
            (metavar "ORIGIN")


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
    txt =   "There are specific instructions available for each command;\n" ++
            "use vault COMMAND --help for details."
