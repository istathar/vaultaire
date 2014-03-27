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

import Control.Concurrent.MVar
import Control.Monad (void, when)
import FilerDaemon (commandLineParser, program)
import GHC.Conc
import Options.Applicative (execParser)
import System.Posix.Signals

main :: IO ()
main = do
    let n = numCapabilities     -- command line +RTS -Nn -RTS value
    when (n == 1) (getNumProcessors >>= setNumCapabilities)

    options <- execParser commandLineParser
    quit_mvar <- newEmptyMVar

    void $ installHandler sigINT (quitHandler quit_mvar) Nothing
    void $ installHandler sigTERM (quitHandler quit_mvar) Nothing

    program options quit_mvar

  where
    quitHandler mv = Catch $ putMVar mv ()
