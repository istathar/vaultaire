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

module Main where

import GHC.Conc
import Options.Applicative (execParser)

import IngestDaemon (commandLineParser, program)

main :: IO ()
main = do
    n <- getNumProcessors
    setNumCapabilities n
    execParser commandLineParser >>= program
