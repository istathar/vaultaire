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

module Main where

import Control.Monad (forever)
import qualified Data.ByteString.Char8 as S
import System.Environment (getArgs)
import System.ZMQ4.Monadic

main :: IO ()
main = do
    args <- getArgs

    let worker = if length args == 1
                    then head args
                    else error "Specify broker hostname or IP address on command line"


    runZMQ $ do
        telem <- socket Sub
        connect telem  ("tcp://" ++ worker ++ ":5570")
        subscribe telem ""

        forever $ do
            message' <- receive telem

            liftIO $ S.putStrLn message'

