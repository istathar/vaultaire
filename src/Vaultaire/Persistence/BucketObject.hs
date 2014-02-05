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
{-# LANGUAGE PackageImports    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Vaultaire.Persistence.BucketObject (
    formObjectLabel,
    appendVaultPoints,
    readVaultObject,

    -- for testing
    tidyOriginName
) where

import Blaze.ByteString.Builder
import Control.Exception
import "mtl" Control.Monad.Error ()
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Char
import Data.Locator
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Serialize
import Data.Word
import System.Rados

import Vaultaire.Conversion.Reader
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
formObjectLabel :: Origin -> ByteString -> Timestamp -> Label
formObjectLabel o' s' t =
    Label $ S.intercalate "_" [__EPOCH__, o', s', t']
  where
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
-- | Given a collection of points in the same source, write them down to Ceph.
--


--
-- The origin contents file is locked before entering here. Build a map of
-- labels to encoded points, then construct a list of asynchronous appends.
--
appendVaultPoints :: Map Label Builder -> Pool ()
appendVaultPoints m = do
    asyncs <- sequence $ Map.foldrWithKey asyncAppend [] m
    mapM_ checkError asyncs
  where
    asyncAppend l bB as =
      let
        l' = runLabel l
      in
        (runAsync . runObject l' $ append $ toByteString bB) : as

    checkError write_in_flight = do
        maybe_error <- waitSafe write_in_flight
        case maybe_error of
            Just err    -> liftIO $ throwIO err
            Nothing     -> return ()

{-
    This whole thing is a bit crazy. We should just merge it all into a single
    use of Data.Serialize.Get
-}

readVaultObject
    :: Origin
    -> SourceDict
    -> Timestamp
    -> Pool (Map Timestamp Point)
readVaultObject o' s t =
    let
        s' = hashSourceDict s           -- FIXME lookup from Directory
        l  = formObjectLabel o' s' t
        l' = runLabel l

    in do
        ey' <- runObject l' readFull    -- Pool (Either RadosError ByteString)

        case ey' of
            Left err        -> liftIO $ throwIO err
            Right y'        -> either error return $ process y' Map.empty

    where

--
-- First write wins. This is a crucial design property; we DO expect duplicate
-- writes as a consequence of the distributed system design of Vaultaire:
-- points are idempotent for a given timestamp; if we see another no need to
-- insert it. This also has an important security aspect: someone can
-- maliciously write later, but we will ignore it and thereby not destroy data.
--

        process :: ByteString -> Map Timestamp Point -> Either String (Map Timestamp Point)
        process y' m1 =
            if S.null y'
                then return m1
                else do
                    (p,z') <- readPoint2 y'
                    let k = timestamp p
                    let m2 = if Map.member k m1
                            then m1
                            else Map.insert k p m1

                    process z' m2


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



