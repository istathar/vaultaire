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

-- | This is a way for vaultaire components to store data within the Vault
-- itself.
module Vaultaire.InternalStore
(
    writeTo,
    readFrom,
    enumerateOrigin
) where

import Control.Monad.State.Strict
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Monoid
import Data.Packer
import Data.Time
import Data.Word (Word64)
import Pipes
import Pipes.Parse
import qualified Pipes.Prelude as Pipes
import System.Log.Logger
import Vaultaire.CoreTypes
import Vaultaire.Daemon (Daemon)
import Vaultaire.Reader (ReadDetails (..), Request (..), readExtended)
import Vaultaire.Writer (BatchState (..), appendExtended, write)

-- | Given an origin and an address, write the given bytes.
writeTo :: Origin -> Address -> ByteString -> Daemon ()
writeTo origin addr payload = do
    liftIO $ errorM "hai" $ "writing : " ++ show (origin, addr, payload)
    runEffect (yield makeState >-> write origin)
  where
    makeState :: BatchState
    makeState =
        let zt     = UTCTime (ModifiedJulianDay 0) 0 in -- kind of dumb
        let empty  = BatchState mempty mempty mempty mempty 0 0 mempty zt in
        let bucket = calculateBucketNumber internalStoreBuckets addr in
        let len    = fromIntegral $ BS.length payload in
        execState (appendExtended 0 bucket addr 0 len payload) empty

-- | To save bootstrapping the system with actual day map files we will simply
-- mod this value. This could be a scaling issue with huge data sets.
internalStoreBuckets :: Word64
internalStoreBuckets = 128

-- | Given an origin and an address, read the avaliable bytes.
readFrom :: Origin -> Address -> Daemon (Maybe ByteString)
readFrom origin addr =
    evalStateT draw $ yield (0, internalStoreBuckets)
                      >-> readExtended origin makeRequest fail_f
                      >-> Pipes.map extractPayload
  where
    extractPayload p = flip runUnpacking p $ do
        unpackSetPosition 16
        len <- getWord64LE
        getBytes (fromIntegral len)

    makeRequest = Extended (ReadDetails addr 0 0)
    fail_f = liftIO . errorM "InternalStore.readFrom" . BS.unpack

-- | Provide a Producer of address and payload tuples.
enumerateOrigin :: Origin -> Producer (Address, ByteString) Daemon ()
enumerateOrigin = undefined

--internalStoreLockOID :: Origin -> ByteString
--internalStoreLockOID (Origin o') = S.append ["02_", o', "_internal"]
