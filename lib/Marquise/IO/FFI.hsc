--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

#include <sys/file.h>

module Marquise.IO.FFI
(
    tryLock,
    c_sync,
) where

import Control.Exception(onException)
import Data.Bits
import Foreign.C.Error
import Foreign.C.Types
import System.Posix.Files
import System.Posix.IO (openFd, closeFd, defaultFileFlags, OpenMode(..))
import System.Posix.Types


open :: FilePath -> IO Fd
open path = openFd path ReadOnly (Just stdFileMode) defaultFileFlags


-- Some locking bits were inspired by Takano Akio's filelock package
tryLock :: FilePath -> IO (Maybe Fd)
tryLock path = do
  fd <- open path
  (`onException` closeFd fd) $ do
    locked <- flock fd
    if locked
      then return $ Just fd
      else closeFd fd >> return Nothing

flock :: Fd -> IO Bool
flock fd@(Fd fd_int) = do
  r <- c_flock fd_int (#{const LOCK_EX} .|. #{const LOCK_NB})
  if r == 0
    then return True -- success
    else do
      errno <- getErrno
      case () of
        _ | errno == eWOULDBLOCK
            -> return False
          | errno == eINTR
            -> flock fd
          | otherwise -> throwErrno "flock"

foreign import ccall "flock"
  c_flock :: CInt -> CInt -> IO CInt

foreign import ccall "unistd.h sync" c_sync :: IO ()
