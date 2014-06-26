{-# LANGUAGE MultiParamTypeClasses #-}
--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

-- | Marquise server library, for transmission of queued data to the vault.
module Marquise.Server
(
    marquiseServer,
    parseContentsRequests,
) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Exception (throw, throwIO)
import Control.Monad.State
import Data.Attoparsec (Parser)
import qualified Data.Attoparsec as Parser
import Data.Attoparsec.ByteString.Lazy (eitherResult, maybeResult, parse)
import Data.Attoparsec.Combinator (eitherP, many')
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Data.Maybe
import Data.Packer
import Data.Word (Word64)
import Marquise.Classes
import Marquise.Client (makeSpoolName, updateSourceDict)
import Marquise.Types (SpoolName (..))
import Pipes
import Vaultaire.Types
import Vaultaire.Util

data ContentsRequest = ContentsRequest Address SourceDict
  deriving Show

marquiseServer :: String -> Origin -> String -> IO ()
marquiseServer broker origin user_sn =
    case makeSpoolName user_sn of
        Left e -> throwIO e
        Right sn -> do
            createDirectories sn
            linkThread (sendPoints broker origin sn)
            linkThread (sendContents broker origin sn)
            waitForever

sendPoints :: String -> Origin -> SpoolName -> IO ()
sendPoints broker origin sn = forever $ do
    (bytes, seal) <- nextPoints sn
    runEffect $ for (breakInToChunks bytes)
                    (lift . transmitBytes broker origin)
    seal

    threadDelay idleTime

sendContents :: (MarquiseContentsMonad m conn, MarquiseSpoolFileMonad m)
             => String
             -> Origin
             -> SpoolName
             -> m ()
sendContents broker origin sn = forever $ do
    (bytes, seal) <- nextContents sn
    withContentsConnection broker $ \c ->
        runEffect $ for (parseContentsRequests bytes)
                        (lift . sendSourceDictUpdate c)
    seal
  where
    sendSourceDictUpdate conn (ContentsRequest addr source_dict) =
        updateSourceDict addr source_dict origin conn >>= either throw return

parseContentsRequests :: Monad m => L.ByteString -> Producer ContentsRequest m ()
parseContentsRequests bytes
    | L.null bytes = return ()
    | otherwise =
        case eitherResult (parse oneRequest bytes) of
            Left e -> error ("parseContentsRequests: " ++ e)
            Right (one, rest) -> yield one >> parseContentsRequests rest

oneRequest :: Parser (ContentsRequest, L.ByteString)
oneRequest = do
    addr <- fromWire <$> Parser.take 8
    len <- runUnpacking getWord64LE <$> Parser.take 8
    source_dict <- fromWire <$> Parser.take (fromIntegral len)
    remainder <- Parser.takeLazyByteString
    case ContentsRequest <$> addr <*> source_dict of
        Left e -> fail (show e)
        Right request -> return (request, remainder)

idleTime :: Int
idleTime = 1000000 -- 1 second

breakInToChunks :: Monad m => L.ByteString -> Producer S.ByteString m ()
breakInToChunks bs
    | L.null bs =
        return ()
    | otherwise =
        let (chunk, remainder) = verifySplit bs
        in yield chunk >> breakInToChunks remainder

-- | Verify that the data is valid, we have to do this verification to split at
-- a valid boundary anyway.
verifySplit :: L.ByteString -> (S.ByteString, L.ByteString)
verifySplit = fromMaybe (error "verifySplit: impossible due to many'")
                        . maybeResult . parse verify
  where
    verify = (,) <$> (L.toStrict . L.fromChunks <$> chunks)
                 <*> Parser.takeLazyByteString
    -- Yes, a linked list of bytestrings isn't the most efficient structure.
    -- It's more than fast enough.
    chunks :: Parser [S.ByteString]
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
                        return $ S.append packet bytes
                    Right () ->
                        error $ "verifySplit: corrupt data (extended burst) at: "
                                ++ show current_size
            Nothing -> do
                put (current_size + 24)
                return packet

    extendedSize :: S.ByteString -> Maybe Int
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
idealBurstSize =  16 * 1048576
