--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.SpoolFile
(
    spoolDir,
) where

import qualified Data.ByteString as S
import Marquise.Classes
import Marquise.IO.FFI
import Marquise.Types
import System.Directory
import System.FilePath.Posix
import System.IO
import System.Posix.Temp

-- This could be more efficient if the handle were kept in a "global
-- variable", using the noinline IORef hack.
instance MarquiseSpoolFileMonad IO where
    createSpoolFile sn = do
        createDirectoryIfMissing True (spoolDir sn)
        (tmp_path, tmp_handle) <- mkstemp (spoolFileTemplate sn)
        hClose tmp_handle
        return $ SpoolFile tmp_path

    append = S.appendFile . unSpoolFile

    close _ = c_sync

spoolFileTemplate :: SpoolName -> String
spoolFileTemplate ns = joinPath [spoolDir ns, "data_"]

spoolDir :: SpoolName -> String
spoolDir (SpoolName sn) = joinPath ["/var/spool/marquise/current/", sn]

