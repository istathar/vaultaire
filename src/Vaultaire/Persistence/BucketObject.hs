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

{-# LANGUAGE InstanceSigs      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-orphans #-}

module Vaultaire.Persistence.BucketObject (
    formObjectLabel,

    -- for testing
    tidyOriginName
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Char
import Data.Locator
import Data.Map.Strict (Map)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text.Encoding as T
import Data.Word
import System.Rados

import qualified Vaultaire.Internal.CoreTypes as Core
import Vaultaire.Persistence.Constants

{-
    I'd really like to think there's an easier way of doing constants
-}

windowSize :: Word64
windowSize = fromIntegral __WINDOW_SIZE__

nanoseconds :: Word64
nanoseconds = fromIntegral $ (1000000000 :: Int)

--
-- Use the relevant information from a point to find out what bucket
-- it belongs in.
--
formObjectLabel :: Core.Point -> S.ByteString
formObjectLabel p =
    S.intercalate "_" [__EPOCH__, o', s', t']
  where
    o' = Core.origin p
    s' = hashSourceDict $ Core.source p
    t  = (Core.timestamp p) `div` (windowSize * nanoseconds)
    t' = S.pack $ show (t * windowSize)


tidyOriginName :: S.ByteString -> S.ByteString
tidyOriginName o' =
  let
    width = 10

    predicate :: Char -> Bool
    predicate c = isAscii c && isPrint c && (c /= '_')

    n' = S.append (S.filter predicate o') (S.replicate width ':')
  in
    S.take width n'


hashOriginName :: ByteString -> ByteString
hashOriginName o' =
    hashStringToLocator16a 6 o'


--
-- | The source dictionary portion of the bucket label is formed as follows:
--
-- 1. Sources are in a Data.Map which is a sorted map, per Ord order.
-- 2. Map is serialized to bytes by __cereal__'s "Data.Serialize.encode"
-- 3. The bytes are hashed with SHA1
-- 4. The hash is converted to 27 digits of base62
--
hashSourceDict :: Map Text Text -> ByteString
hashSourceDict m =
  let
    m' = encode m
  in
    hashStringToBase62 27 m'


instance Serialize Text where
--  put :: Text -> Put
    put t = putByteString $ T.encodeUtf8 t

--  get :: Get Text
    get = do
        x' <- getByteString 0
        return $ T.decodeUtf8 x'


writeVaultPoint :: Core.Point -> Pool -> IO ()
writeVaultPoint _ _ = do
    return ()

