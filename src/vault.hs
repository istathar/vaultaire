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


data Sample = Sample {
    hello :: String,
    quiet :: Bool
}

sample :: Parser Sample
sample = Sample
    <$> strOption
            (long "hello" <> metavar "TARGET" <> help "Target for the greeting")
    <*> switch
            (long "quiet" <> help "Whether to be quiet")


greet :: Sample -> IO ()
greet (Sample h False) = putStrLn $ "Hello " ++ h
greet _ = return ()


main = execParser opts >>= greet
  where
    opts = info (helper <*> sample)
            (fullDesc
                <> progDesc "Print a greeting for TARGET"
                <> header "A test of optparse-applicative")
