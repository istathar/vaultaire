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
{-# OPTIONS -fno-warn-type-defaults #-}

module Vaultaire.Persistence.BucketObject (
    formObjectLabel,
    appendVaultPoint,
    readVaultObject,

    -- for testing
    tidyOriginName
) where

import Control.Monad.Error ()
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Char
import Data.Locator
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Serialize
import Data.Text (Text)
import qualified Data.Text.Encoding as T
import Data.Word
import qualified System.Rados as Rados

import Vaultaire.Conversion.Reader
import Vaultaire.Conversion.Writer
import Vaultaire.Internal.CoreTypes
import Vaultaire.Persistence.Constants
import qualified Vaultaire.Serialize.DiskFormat as Disk

{-
    I'd really like to think there's an easier way of doing constants
-}

windowSize :: Word64
windowSize = fromIntegral __WINDOW_SIZE__

--
-- Use the relevant information from a point to find out what bucket
-- it belongs in.
--
formObjectLabel :: Origin -> SourceDict -> Word64 -> ByteString
formObjectLabel o' s t =
    S.intercalate "_" [__EPOCH__, o', s', t']
  where
    s' = hashSourceDict s
    t2 = t `div` (windowSize * nanoseconds)
    t' = S.pack $ show (t2 * windowSize)


tidyOriginName :: ByteString -> ByteString
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
hashSourceDict :: SourceDict -> ByteString
hashSourceDict s =
  let
    m' = encode s
  in
    hashStringToBase62 27 m'


instance Serialize SourceDict where
--  put :: a -> Put
    put x =
      let
        m = runSourceDict x
      in
        put m

--  get :: Get a
    get = do
        m <- get
        return $ SourceDict (m :: Map Text Text)


instance Serialize Text where
--  put :: Text -> Put
    put t = putByteString $ T.encodeUtf8 t

--  get :: Get Text
    get = do
        x' <- getByteString 0
        return $ T.decodeUtf8 x'


--
-- | Given a single data point, write it down to Ceph. We express as a
-- VaultPoint protobuf, serialize to bytes, then prepend a VaultPrefix
-- in order to store the size necessary to be able to read it back again.
--

appendVaultPoint :: Rados.Pool -> Point -> IO ()
appendVaultPoint pool p =
    let
        p' = encodePoint p
        r  = createDiskPrefix (fromIntegral $ S.length p')
        r' = encode r

        b' = S.concat [r',p']

        o' = origin p
        s  = source p
        t  = timestamp p

        l' = formObjectLabel o' s t
    in do
        Rados.withSharedLock pool l' "name" "desc" "tag" (Just 1)
            (Rados.syncAppend pool l' b')

{-
    Marshalling into a Point just to form a bucket label is a bit silly.

    This whole thing is a bit crazy. We should just merge it all into a single
    use of Data.Serialize.Get
-}

readVaultObject :: Rados.Pool -> Origin -> SourceDict -> Timestamp -> IO (Map Timestamp Point)
readVaultObject pool o' s t =
    let
        l' = formObjectLabel o' s t

    in do
        y' <- Rados.syncRead pool l' 0 (2 ^ 22)                 -- FIXME size

        either error return (process y' Map.empty)
    where

--
-- First write wins. This is a crucial design property; we DO expect duplicate
-- writes as a consequence of the distributed system design of Vaultaire:
-- points are idempotent for a given timestamp; if we see another no need to
-- insert it. This also has an important security aspect: someone can
-- maliciously write later, but we will ignore it and thereby not destroy data.
--

        process :: ByteString -> Map Timestamp Point -> Either String (Map Timestamp Point)
        process y' m1 = do
            (p,z') <- readPoint2 y'
            let k = timestamp p
            let m2 = if Map.member k m1
                    then m1
                    else Map.insert k p m1
            if S.null z'
                then return m2
                else process z' m2


        readPoint2 :: ByteString -> Either String (Point, ByteString)
        readPoint2 x' = do
            ((VaultRecord _ pb), remainder') <- runGetState get x' 0
            return (convertToPoint o' s pb, remainder')


data VaultRecord = VaultRecord Disk.VaultPrefix Disk.VaultPoint

instance Serialize VaultRecord where
    put (VaultRecord prefix point) = do
        put prefix
        put point

    get = do
        prefix <- get
        let len = fromIntegral $ Disk.size prefix
        point <- isolate len get
        return $ VaultRecord prefix point



