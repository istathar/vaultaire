--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE ForeignFunctionInterface #-}

module Marquise.IO.SpoolFile
(
    dataFilePath,
    spoolDir,
) where

import qualified Data.ByteString.Lazy as LB
import Marquise.Classes
import Marquise.Types
import Data.Word(Word64)

-- This could be more efficient if the handle were kept in a "global
-- variable", using the noinline IORef hack.
instance MarquiseSpoolFileMonad IO where
    append ns = LB.appendFile (dataFilePath ns)
    close _ = c_sync

dataFilePath :: SpoolName -> String
dataFilePath (SpoolName ns) = spoolDir ++ ns

-- Trailing slash is important
spoolDir :: FilePath
spoolDir = "/var/spool/marquise/"


tmpTemplate :: SpoolName -> String
tmpTemplate ns = dataFilePath ns ++ "_"

foreign import ccall "unistd.h sync" c_sync :: IO ()

