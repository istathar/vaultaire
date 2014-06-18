--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.Writer
(
    tryWorkDir
) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Exception
import Control.Monad.State
import Data.Attoparsec (Parser)
import qualified Data.Attoparsec as Parser
import Data.Attoparsec.ByteString.Lazy (maybeResult, parse)
import Data.Attoparsec.Combinator (eitherP, many')
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as LB
import Data.Maybe
import Data.Packer
import Data.Word (Word64)
import Marquise.Classes
import Marquise.IO.Connection
import Marquise.IO.FFI
import Marquise.IO.SpoolFile (spoolDir)
import Marquise.Types
import System.Directory
import System.FilePath.Posix
import System.IO
import System.IO.Unsafe
import System.Posix.Files
import System.Posix.IO (closeFd)
import System.Posix.Temp
import System.Posix.Types (Fd)
import Vaultaire.Types

instance MarquiseWriterMonad IO where
    nextBurst sn = do
        createDirectoryIfMissing True (spoolDir sn)
        createDirectoryIfMissing True (workDir sn)
        -- First check for any work already in the work spool dir.
        work <- tryWorkDir sn
        case work of
            Nothing ->
                -- No existing work, get some new work out of the spool
                -- directory then.
                rotateSpoolDir sn >> nextBurst sn
            Just (fp, lock_fd) -> do
                contents <- LB.readFile fp
                let close_f = removeLink fp >> closeFd lock_fd
                return (contents, close_f)

    transmitBytes broker origin bytes =
        withConnection ("tcp://" ++ broker ++ ":5560") $ \c -> do
            send (PassThrough bytes) origin c
            result <- recv c
            case result of
                Left e -> throw e
                Right OnDisk -> return ()
                Right InvalidWriteOrigin -> error "send: Invalid origin"

-- | Check the work directory for any outstanding work, if there is a potential
-- candidate, lock it. If that fails, try the next.
tryWorkDir :: SpoolName -> IO (Maybe (FilePath, Fd))
tryWorkDir sn =
    listToMaybe . catMaybes <$> (getAbsoluteDirectoryFiles (workDir sn)
                                 >>= mapM lazyLock)
  where
    lazyLock :: FilePath -> IO (Maybe (FilePath, Fd))
    lazyLock fp = unsafeInterleaveIO $ do
        lock <- tryLock fp
        case lock of
            Nothing -> return Nothing
            Just lock_fd -> return . Just $ (fp, lock_fd)

getAbsoluteDirectoryFiles :: FilePath -> IO [FilePath]
getAbsoluteDirectoryFiles =
    getAbsoluteDirectoryContents >=> filterM doesFileExist

getAbsoluteDirectoryContents :: FilePath -> IO [FilePath]
getAbsoluteDirectoryContents fp =
    map (\rel -> joinPath [fp, rel]) <$> getDirectoryContents fp

workDir :: SpoolName -> FilePath
workDir (SpoolName sn) = joinPath ["/var/spool/marquise/work", sn]

workTemplate :: SpoolName -> FilePath
workTemplate sn = joinPath [workDir sn, "data_"]

rotateSpoolDir :: SpoolName -> IO ()
rotateSpoolDir sn = do
    works <- getAbsoluteDirectoryFiles (spoolDir sn)
    case works of
        [] -> wait >> rotateSpoolDir sn
        x:_ -> do
            (tmp_path, tmp_handle) <- mkstemp (workTemplate sn)
            hClose tmp_handle
            renameFile x tmp_path
            wait
  where
    wait = threadDelay 1000000



-- | Verify that the data is valid, we have to do this verification to split at
-- a valid boundary anyway.
verifySplit :: LB.ByteString -> (ByteString, LB.ByteString)
verifySplit = fromMaybe (error "verifySplit: impossible due to many'")
                        . maybeResult . parse verify
  where
    verify = (,) <$> (LB.toStrict . LB.fromChunks <$> chunks)
                 <*> Parser.takeLazyByteString
    -- Yes, a linked list of bytestrings isn't the most efficient structure.
    -- It's more than fast enough.
    chunks :: Parser [ByteString]
    chunks = flip evalStateT 0 $ many' $ do
        current_size <- get
        when (current_size > idealBurstSize) (lift $ fail "I am full now.")

        packet <- lift $ Parser.take 24

        case extendedSize packet of
            Just len -> do
                -- Mast ensure that we get this many bytes now, or attoparsec
                -- will just backtrack on us. We do this with a dummy parser
                -- inside an eitherP
                extended <- lift $ eitherP (Parser.take len) (return ())
                case extended of
                    Left bytes -> do
                        put (current_size + fromIntegral len + 24)
                        return $ BS.append packet bytes
                    Right () ->
                        error $ "verifySplit: corrupt data (extended burst) at: "
                                ++ show current_size
            Nothing -> do
                put (current_size + 24)
                return packet

    extendedSize :: ByteString -> Maybe Int
    extendedSize packet = flip runUnpacking packet $ do
        addr <- Address <$> getWord64LE
        if isAddressExtended addr
            then do
                unpackSkip 8
                Just . fromIntegral <$> getWord64LE -- length
            else
                return Nothing



-- A burst should be, at maximum, very close to this side, unless the user
-- decides to send a very long extended point.
idealBurstSize :: Word64
idealBurstSize = maxBound -- 1048576 -- 1MB
