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
import qualified Data.ByteString.Char8 as S
import qualified Data.Map.Strict as Map
import Data.Word (Word64)
import Options.Applicative
import Data.Map (Map)
import Data.Text (Text)
import qualified Data.Text as T

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket

import Debug.Trace

data Options = Options {
    optGlobalQuiet :: Bool,
    optCommand     :: Command
}

data Command
    = ReadCommand ReadOptions
    | ContentsCommand ContentsOptions

data ReadOptions = ReadOptions {
    argReadOrigin    :: String,
    argReadSource    :: String,
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
            (command "read" (ReadCommand <$> readParser) <>
             command "contents" (ContentsCommand <$> contentsParser))


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



commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Operations on a Vaultaire data vault." <>
                header "A data vault for metrics" <>
                footer txt)
  where
    txt =   "There are specific instructions available for each command;\n" ++
            "use vault COMMAND --help for details."


handleSourceArgument :: String -> Map Text Text
handleSourceArgument arg =
  let
    arg' = S.pack arg
    pairs' = S.split ',' arg'
    sources' = map (S.split ':') pairs'
    sources = map toTag sources'
  in
    Map.fromList sources
  where
    toTag [k',v'] = (T.pack $ S.unpack k', T.pack $ S.unpack v')

program :: Options -> IO ()
program (Options _ cmd) =
    case cmd of
        ReadCommand (ReadOptions o s t)        -> do
            let tags = handleSourceArgument s

            let p = Point {
                origin = S.pack o,
                source = tags,
                timestamp = read t :: Word64,
                payload = Empty
            }

            let l' = traceShow p $ Bucket.formObjectLabel p
            print l'

        ContentsCommand (ContentsOptions o)    -> print [o]


main = execParser commandLineParser >>= program
