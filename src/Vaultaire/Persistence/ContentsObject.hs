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

module Vaultaire.Persistence.ContentsObject (
    formObjectLabel,
    appendVaultSource,
    readVaultObject
) where

import Control.Exception
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Serialize
import Data.Set (Set)
import qualified Data.Set as Set
import System.Rados

import Vaultaire.Conversion.Reader
import Vaultaire.Conversion.Writer
import Vaultaire.Internal.CoreTypes
import Vaultaire.Persistence.Constants
import qualified Vaultaire.Serialize.DiskFormat as Disk


--
-- For each origin, we maintain a list of known sources. This is the name of
-- the object we store it in
--
formObjectLabel :: Origin -> ByteString
formObjectLabel o' =
    S.intercalate "_" [__EPOCH__, o', __CONTENTS__]


appendVaultSource :: Origin -> SourceDict -> Pool ()
appendVaultSource o' s =
    let
        s' = encode $ createDiskContent s
        r' = encode $ createDiskPrefix (fromIntegral $ S.length s')

        b' = S.concat [r',s']

        l' = formObjectLabel o'
    in do
        em <- withSharedLock l' "name" "desc" "tag" (Just 1)
            (runObject l' $ append b')

        case em of
            Just err    -> liftIO $ throwIO err
            Nothing     -> return ()


readVaultObject :: Origin -> Pool (Set SourceDict)
readVaultObject o' =
    let
        l' = formObjectLabel o'

    in do
        ey' <- runObject l' readFull

        case ey' of
            Left err        -> liftIO $ throwIO err
            Right y'        -> either error return $ process y' Set.empty

    where

        process :: ByteString -> Set SourceDict -> Either String (Set SourceDict)
        process y' e1 = do
            (s,z') <- readSource y'

            let e2 = if Set.member s e1
                    then e1
                    else Set.insert s e1

            if S.null z'
                then return e2
                else process z' e2


        readSource :: ByteString -> Either String (SourceDict, ByteString)
        readSource x' = do
            ((VaultRecord _ sb), z') <- runGetState get x' 0
            return (convertToSource sb, z')


data VaultRecord = VaultRecord Disk.VaultPrefix Disk.VaultContent

instance Serialize VaultRecord where
    put (VaultRecord pre s) = do
        put pre
        put s

    get = do
        pre <- get
        let len = fromIntegral $ Disk.size pre
        s <- isolate len get
        return $ VaultRecord pre s



