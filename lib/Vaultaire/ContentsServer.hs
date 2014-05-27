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
    -- testing
    encodeAddressToBytes,
    decodeAddressFromBytes,
    encodeContentsListEntry
) where

import Control.Applicative
import Control.Exception
import Control.Monad.State.Strict
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.Maybe (isJust)
import Data.Monoid (mempty)
import Data.Packer
import Data.Word (Word64)
import Pipes
import System.Random
import Vaultaire.WireFormats.ContentsOperation (ContentsOperation (..),
                                                SourceDict)
import Vaultaire.WireFormats.SourceDict (diffSource, unionSource)
import Vaultaire.WireFormats.Class (fromWire, toWire)

import Vaultaire.CoreTypes
import Vaultaire.Daemon
import qualified Vaultaire.InternalStore as InternalStore

-- | Start a writer daemon, never returns.
startContents
    :: String           -- ^ Broker
    -> Maybe ByteString -- ^ Username for Ceph
    -> ByteString       -- ^ Pool name for Ceph
    -> IO ()
startContents broker user pool =
    runDaemon broker user pool $ forever $ nextMessage >>= handleRequest


handleRequest :: Message -> Daemon ()
handleRequest (Message reply origin payload) =
    case fromWire payload of
        Left err         -> failWithString reply "Unable to parse request message" err
        Right op -> case op of
            ContentsListRequest   -> performListRequest reply origin
            GenerateNewAddress    -> performRegisterRequest reply origin
            UpdateSourceTag a s   -> performUpdateRequest reply origin a s
            RemoveSourceTag a s   -> performRemoveRequest reply origin a s

failWithString :: (Response -> Daemon ()) -> String -> SomeException -> Daemon ()
failWithString reply msg e = do
    liftIO $ putStrLn $ msg ++ "; " ++ show e
    reply (Failure (S.pack msg))


{-
    For the given address, read all the contents entries matching it. The
    latest entry is deemed most correct. Return that blob. No attempt is made
    to decode it; after all, the only way it could get in there is via the
    update or remove opcodes.

    The use of a Pipe here allows us to stream the responses back to the
    requesting client. Note that reply with Response can be used multiple
    times, so each reply here represents one Address,SourceDict pair.
-}
performListRequest :: (Response -> Daemon ()) -> Origin ->  Daemon ()
performListRequest reply o =
    runEffect $
        for (InternalStore.enumerateOrigin o) (lift . reply . Response . encodeContentsListEntry)


performRegisterRequest :: (Response -> Daemon ()) -> Origin -> Daemon ()
performRegisterRequest reply o =
    allocateNewAddressInVault o
    >>= reply . Response . encodeAddressToBytes


allocateNewAddressInVault :: Origin -> Daemon Address
allocateNewAddressInVault o = do
    num <- liftIO rollDice
    let a = Address (num `clearBit` 0)

    withExLock "02_addresses_lock" $ do
        exists <- isAddressInVault o a
        if exists
            then allocateNewAddressInVault o
            else do
                writeSourceTagsForAddress o a mempty
                return a
  where
        rollDice = getStdRandom (randomR (0, maxBound :: Word64))


encodeAddressToBytes :: Address -> ByteString
encodeAddressToBytes (Address a) = runPacking 8 (putWord64LE a)

decodeAddressFromBytes :: ByteString -> Address
decodeAddressFromBytes = Address . runUnpacking getWord64LE


encodeContentsListEntry :: (Address, ByteString) -> ByteString
encodeContentsListEntry (Address a, x') =
  let
    len  = B.length x'
    size = 8 + 8 + len
  in
    runPacking size $ do
        putWord64LE a
        putWord64LE (fromIntegral len)
        putBytes x'

performUpdateRequest
    :: (Response -> Daemon ())
    -> Origin
    -> Address
    -> SourceDict
    -> Daemon ()
performUpdateRequest reply o a s = do
    s' <- retreiveSourceTagsForAddress o a
    writeSourceTagsForAddress o a (unionSource s s')
    reply Success


isAddressInVault :: Origin -> Address -> Daemon Bool
isAddressInVault o a =
    isJust <$> InternalStore.readFrom o a


retreiveSourceTagsForAddress :: Origin -> Address -> Daemon SourceDict
retreiveSourceTagsForAddress o a = do
    result <- InternalStore.readFrom o a
    return $ case result of
        Just b'     -> either throw id (fromWire b')
        Nothing     -> mempty


writeSourceTagsForAddress :: Origin -> Address -> SourceDict -> Daemon ()
writeSourceTagsForAddress o a s =
    InternalStore.writeTo o a (toWire s)


performRemoveRequest
    :: (Response -> Daemon ())
    -> Origin
    -> Address
    -> SourceDict
    -> Daemon ()
performRemoveRequest reply o a s = do
    s' <- retreiveSourceTagsForAddress o a
    writeSourceTagsForAddress o a $ diffSource s' s
    reply Success
