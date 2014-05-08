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

{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.ContentsServer
(
    startContents,
    Operation(..),
    -- testing
    opcodeToWord64
) where

import Control.Exception
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Text (Text)
import qualified Data.Text as T
import Control.Monad.IO.Class
import Data.Packer
import Data.Word (Word64)
import Control.Monad (forever)

import Vaultaire.Daemon
import Vaultaire.ContentsEncoding

--
-- Daemon implementation
--

data Operation =
    ContentsListRequest |
    RegisterNewAddress |
    UpdateSourceTag |
    RemoveSourceTag
  deriving
    (Show, Eq, Enum)



-- | Start a writer daemon, never returns.
startContents
    :: String           -- ^ Broker
    -> Maybe ByteString -- ^ Username for Ceph
    -> ByteString       -- ^ Pool name for Ceph
    -> IO ()
startContents broker user pool =
    runDaemon broker user pool $ forever $ nextMessage >>= handleRequest


handleRequest :: Message -> Daemon ()
handleRequest (Message reply p _) =
    case tryUnpacking parseOperationMessage p of
        Left err    -> failWithString reply "Unable to parse request message" err
        Right op    -> case op of
            ContentsListRequest -> performListRequest
            RegisterNewAddress  -> performRegisterRequest
            UpdateSourceTag     -> performUpdateRequest
            RemoveSourceTag     -> performRemoveRequest


parseOperationMessage :: Unpacking Operation
parseOperationMessage = do
    opcode <- getWord64LE
    return $ toEnum $ fromIntegral opcode


failWithString :: (Response -> Daemon ()) -> String -> SomeException -> Daemon ()
failWithString reply msg e = do
    liftIO $ putStrLn $ msg ++ "; " ++ show e
    reply (Failure (S.pack msg))


opcodeToWord64 :: Operation -> Word64
opcodeToWord64 op = fromIntegral $ fromEnum op


performListRequest :: Daemon ()
performListRequest = undefined


performRegisterRequest = undefined


performUpdateRequest = undefined


performRemoveRequest = undefined
