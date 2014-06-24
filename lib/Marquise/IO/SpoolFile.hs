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
) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Monad.State
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy as LB
import Data.Maybe
import Marquise.Classes
import Marquise.IO.FFI
import Marquise.Types
import System.Directory
import System.FilePath.Posix
import System.IO
import System.IO.Unsafe
import System.Posix.Files
import System.Posix.IO (closeFd)
import System.Posix.Temp
import System.Posix.Types (Fd)

instance MarquiseSpoolFileMonad IO where
    randomSpoolFiles sn =
        SpoolFiles <$> newRandomSpoolFile (newPointsDir sn)
                   <*> newRandomSpoolFile (newContentsDir sn)

    createDirectories sn =
        mapM_ (createDirectoryIfMissing True . ($sn))
              [ newPointsDir
              , newContentsDir
              , curPointsDir
              , curContentsDir]

    appendPoints = doAppend . pointsSpoolFile
    appendContents = doAppend . contentsSpoolFile

    nextPoints sn = nextSpoolContents (newPointsDir sn) (curPointsDir sn)
    nextContents sn = nextSpoolContents (newContentsDir sn) (curContentsDir sn)

    close _ = c_sync

newRandomSpoolFile :: FilePath -> IO FilePath
newRandomSpoolFile path = do
    (spool_file, handle) <- mkstemp path
    hClose handle
    return spool_file

-- | Grab the next avaliable spool file, providing that file as a lazy
-- bytestring and an action to close it, wiping the file.
nextSpoolContents :: FilePath -> FilePath -> IO (L.ByteString, IO ())
nextSpoolContents new_dir cur_dir = do
    -- First check for any work already in the work spool dir.
    work <- tryCurDir cur_dir
    case work of
        Nothing ->
            -- No existing work, get some new work out of the spool
            -- directory then.
            rotate new_dir cur_dir >> nextSpoolContents new_dir cur_dir
        Just (fp, lock_fd) -> do
            threadDelay 100000 -- Ensure that any slow writes are done
            contents <- LB.readFile fp
            let close_f = removeLink fp >> closeFd lock_fd
            return (contents, close_f)

-- | Check the work directory for any outstanding work, if there is a potential
-- candidate, lock it. If that fails, try the next.
tryCurDir :: FilePath -> IO (Maybe (FilePath, Fd))
tryCurDir cur_dir =
    listToMaybe . catMaybes <$> (getAbsoluteDirectoryFiles cur_dir
                                 >>= mapM lazyLock)
  where
    lazyLock :: FilePath -> IO (Maybe (FilePath, Fd))
    lazyLock fp = unsafeInterleaveIO $ do
        lock <- tryLock fp
        case lock of
            Nothing -> return Nothing
            Just lock_fd -> return . Just $ (fp, lock_fd)

-- Attempt to rotate all files from src folder to dst folder. If nothing is ready,
-- wait for a second and retry.
rotate :: FilePath -> FilePath -> IO ()
rotate src dst = do
    candidates <- getAbsoluteDirectoryFiles src
    if null candidates
        then wait >> rotate src dst
        else mapM_ doMove candidates
  where
    doMove src_file = do
        (new_path, h) <- mkstemp dst
        hClose h
        renameFile src_file new_path

    wait = threadDelay 1000000


getAbsoluteDirectoryFiles :: FilePath -> IO [FilePath]
getAbsoluteDirectoryFiles =
    getAbsoluteDirectoryContents >=> filterM doesFileExist

getAbsoluteDirectoryContents :: FilePath -> IO [FilePath]
getAbsoluteDirectoryContents fp =
    map (\rel -> joinPath [fp, rel]) <$> getDirectoryContents fp

doAppend :: FilePath -> ByteString -> IO ()
doAppend = S.appendFile

newPointsDir :: SpoolName -> FilePath
newPointsDir = specificSpoolDir ["points", "new"]

newContentsDir :: SpoolName -> FilePath
newContentsDir = specificSpoolDir ["contents", "new"]

curPointsDir :: SpoolName -> FilePath
curPointsDir = specificSpoolDir ["points", "cur"]

curContentsDir :: SpoolName -> FilePath
curContentsDir = specificSpoolDir ["contents", "cur"]

specificSpoolDir :: [String] -> SpoolName -> FilePath
specificSpoolDir subdirs sn =
    addTrailingPathSeparator (joinPath (baseSpoolDir sn:subdirs))

baseSpoolDir :: SpoolName -> FilePath
baseSpoolDir (SpoolName sn) = joinPath ["/var/spool/marquise/", sn]


