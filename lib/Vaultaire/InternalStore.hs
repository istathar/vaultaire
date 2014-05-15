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

import Data.ByteString (ByteString)
import Pipes
import Vaultaire.Daemon (Address, Daemon)
import Vaultaire.OriginMap (Origin (..))

-- | Given an origin and an address, write the given bytes.
writeTo :: Origin -> Address -> ByteString -> Daemon (Either String ())
writeTo = undefined

-- | Given an origin and an address, read the avaliable bytes.
readFrom :: Origin -> Address -> Daemon (Either String ByteString)
readFrom = undefined

-- | Provide a Producer of address and payload tuples.
enumerateOrigin :: Origin -> Producer (Address, ByteString) Daemon ()
enumerateOrigin = undefined


--internalStoreLockOID :: Origin -> ByteString
--internalStoreLockOID (Origin o') = S.append ["02_", o', "_internal"]
