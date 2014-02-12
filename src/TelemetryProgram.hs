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
{-# LANGUAGE PackageImports     #-}
{-# LANGUAGE RecordWildCards    #-}

module TelemetryProgram where

import Blaze.ByteString.Builder
import Codec.Compression.LZ4
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad
import "mtl" Control.Monad.Error ()
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Time.Clock
import GHC.Conc
import Options.Applicative
import System.ZMQ4.Monadic hiding (source)
import Text.Printf


data Options = Options {
    argDaemonHost     :: !String
}


program :: Options -> IO ()
program (Options daemon) = do
    runZMQ $ do
        telem <- socket Sub
        connect telem  ("tcp://" ++ daemon ++ ":5570")
        subscribe telem ""

        forever $ do
            [k',v'] <- receiveMulti telem
            let k = S.unpack k'
            let v = S.unpack v'
            
            liftIO $ putStrLn $ printf "%-10s %-8s" (k ++ ":") v


toplevel :: Parser Options
toplevel = Options
    <$> argument str
            (metavar "BROKER" <>
             help "Host name or IP address of ingestd to follow")


commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Simple utility to read telemetry from an ingestd" <>
                header "A data vault for metrics")
