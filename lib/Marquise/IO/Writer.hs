--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.Writer
(
) where

import Marquise.Classes
import System.Directory
import Marquise.IO.SpoolFile(dataFilePath)
import System.Posix.Files
import Vaultaire.Types
import Marquise.IO.Connection
import System.Posix.Temp
import Data.ByteString(ByteString)
import qualified Data.ByteString.Lazy as LB
import Data.Word(Word64)
import Control.Exception
import Data.Maybe
import Control.Applicative
import qualified Data.Attoparsec as Parser
import Data.Attoparsec (Parser)
import Data.Attoparsec.ByteString.Lazy (maybeResult, parse)
import Data.Attoparsec.Combinator (eitherP, many')
import Control.Monad.State
import qualified Data.ByteString.Char8 as BS
import System.IO
import Data.Packer 
import Marquise.Types

newtype BurstPath = BurstPath { unBurstPath :: FilePath }
    deriving (Show, Eq)

instance MarquiseWriterMonad IO BurstPath where
    nextBurst ns = do
        exists <- doesFileExist (dataFilePath ns)
        if exists
            then doSwap ns
            else return Nothing

    flagSent = removeLink . unBurstPath

    transmitBytes broker origin bytes =
        withConnection ("tcp://" ++ broker ++ ":5560") $ \c -> do
            send (PassThrough bytes) origin c
            result <- recv c
            case result of
                Left e -> throw e
                Right OnDisk -> return ()
                Right InvalidWriteOrigin -> error "send: Invalid origin"

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


doSwap :: SpoolName -> IO (Maybe (BurstPath, ByteString))
doSwap ns =  do
    -- Create a temp file to atomically move our data into.
    (tmp_path, tmp_handle) <- mkstemp (tmpTemplate ns)
    hClose tmp_handle
    rename (dataFilePath ns) tmp_path

    -- Read the whole file lazily
    parsed <- try $ verifySplit <$> LB.readFile tmp_path
    case parsed of
        Right (burst_data, remainder) -> do
            -- If the file is huge, we want to put the remainder back, this
            -- happens lazily. Larger than memory files should not be an
            -- issue.
            unless (LB.null remainder) $ append ns remainder
            return $ Just (BurstPath tmp_path, burst_data)
        Left (_ :: ErrorCall) ->
            error $ "nextBurst: panic: corrupt data:" ++ tmp_path


-- A burst should be, at maximum, very close to this side, unless the user
-- decides to send a very long extended point.
idealBurstSize :: Word64
idealBurstSize = maxBound -- 1048576 -- 1MB

tmpTemplate :: SpoolName -> String
tmpTemplate ns = dataFilePath ns ++ "_"
