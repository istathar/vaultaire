--
-- Data vault for metrics
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE InstanceSigs      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-orphans #-}

module Vaultaire.Persistence.ObjectFormat (
    formObjectLabel,

    -- for testing
    tidyOriginName
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Char
import Data.Map.Strict (Map)
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text.Encoding as T
import Data.Word

import qualified Vaultaire.Internal.CoreTypes as Core
import Vaultaire.Persistence.Hashes
import Vaultaire.Persistence.Locators

--
-- Epoch version of the bucket object labels. This is only a sanity guard.
-- Different object label versions must be in different pools, as there is (by
-- design) no logic to probe for differnt epoch versions; the code reading a
-- given pool should know the one [and only one] epoch it is valid for.
--

__EPOCH__ :: ByteString
__EPOCH__ = "01"

--
-- Number of seconds per bucket.
--

__WINDOW_SIZE__ :: Int
__WINDOW_SIZE__ = 100000


{-
import qualified Data.Map.Strict as Map

data Bucket = Bucket {
    origin :: Text,
    source2 :: Map Text Text,
    timemark :: Word64      -- seconds since epoch, div 100000
} deriving (Eq, Show)
-}

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
    o' = hashOriginName $ Core.origin p
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


hashOriginName :: S.ByteString -> S.ByteString
hashOriginName o' =
    hashStringToLocator16 6 o'


hashSourceDict :: Map Text Text -> S.ByteString
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
