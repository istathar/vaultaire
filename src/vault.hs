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
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where

import Prelude hiding (mapM)

import Codec.Compression.LZ4
import Control.Applicative
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Foldable
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word64)
import Options.Applicative
import System.Rados

import Vaultaire.Conversion.Receiver
import Vaultaire.Conversion.Transmitter
import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import Vaultaire.Persistence.Constants

data Options = Options {
    optGlobalQuiet :: Bool,
    optCommand     :: Command
}

data Command =
    ReadCommand {
        argReadOrigin    :: String,
        argReadSource    :: String,
        argReadTimestamp :: String
    } |
    ContentsCommand {
        argContentsOrigin :: String
    }


toplevel :: Parser Options
toplevel = Options
    <$> switch
            (long "verbose" <>
             short 'v' <>
             help "Include diagnostic information in output")
    <*> subparser
            (command "read" readParser <>
             command "contents" contentsParser)


readParser :: ParserInfo Command
readParser =
    info (helper <*> readOptions) (progDesc "Dump the values in the bucket containing TIME")


readOptions :: Parser Command
readOptions =
    ReadCommand
    <$> argument str
            (metavar "ORIGIN")
    <*> argument str
            (metavar "SOURCE")
    <*> argument str
            (metavar "TIME")


contentsParser :: ParserInfo Command
contentsParser =
    info (helper <*> contentsOptions) (progDesc "Get the contents list for this origin")


contentsOptions :: Parser Command
contentsOptions =
    ContentsCommand
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

{-
    We can probably replace this with a proper parser in order to get better
    error reporting.
-}
handleSourceArgument :: String -> SourceDict
handleSourceArgument arg =
  let
    arg'   = S.pack arg
    items' = S.split ',' arg'
    pairs' = map (S.split ':') items'
    pairs  = map toTag pairs'
  in
    SourceDict $ Map.fromList pairs
  where
    toTag :: [ByteString] -> (Text, Text)
    toTag [k',v'] = (T.pack $ S.unpack k', T.pack $ S.unpack v')
    toTag _ = error "invalid source argument"


--
-- This is essentially the same code as in the Show instance for Point, but
-- here meant for output to the read command, not debugging.
--
displayPoint :: Point -> IO ()
displayPoint p =
  let
    t = show $ timestamp p
    v = case payload p of
        Empty       -> ""
        Numeric n   ->  show n
        Textual x   ->  T.unpack x
        Measurement r -> show r
        Blob b'     -> toHex b'
  in do
    putStrLn $ t ++ "\t" ++ v




{-
    Some of this code will be refactored to elsewhere, probably
    Vaultaire.Persistence.BucketObject.
-}

program :: Options -> IO ()
program (Options verbose cmd) =
    case cmd of
        ReadCommand o0 s0 t0   -> do
            let o' = S.pack o0

            let s = handleSourceArgument s0

            let t = (read t0 :: Word64) * nanoseconds

--
-- Display the SourceDict used to query, if verbose
--

            debug verbose s

--
-- Determine the appropriate object label, then see if it exists
--


            debug verbose $ Bucket.formObjectLabel o' s t


            m <- withConnection Nothing (readConfig "/etc/ceph/ceph.conf") (\connection ->
                withPool connection "test1" (\pool ->
                    Bucket.readVaultObject pool o' s t))

--
--          Fold over m, print the timestamps + values
--

            traverse_ displayPoint m


        ContentsCommand o   -> print [o]


debug :: Show s => Bool -> s -> IO ()
debug verbose x =
    if verbose
        then do
            putStrLn $ show x
            putStrLn ""
        else
            return ()

main = execParser commandLineParser >>= program
