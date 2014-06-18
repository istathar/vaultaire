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
{-# LANGUAGE PackageImports    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where

import Control.Exception (throw)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Binary.IEEE754 (doubleToWord)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Foldable (forM_)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import System.Environment (getArgs)
import qualified System.Rados.Monadic as Rados
import Text.Printf

import Vaultaire.Internal.CoreTypes
import qualified Vaultaire.Persistence.BucketObject as Bucket
import qualified Vaultaire.Persistence.ContentsObject as Contents

import qualified "vaultaire" Marquise.Client as Marquise
import qualified "vaultaire" Vaultaire.Types as Vaultaire

type InclusionF = Map ByteString ByteString -> ByteString

hashSourceToAddress :: Origin -> SourceDict -> Vaultaire.Address
hashSourceToAddress (Origin o') s = (Marquise.hashIdentifier . inclusions) s
  where
    inclusions = case o' of
                    "ABCDEF"    -> selectInclusionsIpTraf
                    "4HXR1F"    -> selectInclusionsIpTraf
                    "R82KX1"    -> selectInclusionsNagios
                    "LMRH8C"    -> selectInclusionsNagios
                    "ZZZZZZ"    -> passThrough
                    "YCX0H1"    -> error ("TODO which kind of origin is " ++ (S.unpack o'))
                    _           -> error "Origin not configured yet"


passThrough :: SourceDict -> ByteString
passThrough = selectInclusionsIpTraf

selectInclusionsIpTraf :: SourceDict -> ByteString
selectInclusionsIpTraf = S.pack . show

selectInclusionsNagios :: SourceDict -> ByteString
selectInclusionsNagios s =
    S.concat ["host:", host, ",metric:", metric, ",service:", service,","]
  where
    m      = runSourceDict s
    host   = lookfor "hostname"
    metric = lookfor "metric"
    service = lookfor "service_name"

    lookfor :: ByteString -> ByteString
    lookfor k = case Map.lookup k m of
                    Just v  -> v
                    Nothing -> error ("Lookup failed mandatory field \"" ++ (S.unpack k) ++ "\"")


convertSourceDict :: SourceDict -> Vaultaire.SourceDict
convertSourceDict = either error id . Vaultaire.makeSourceDict . makeHashMapFromMap . filterUndesireables . runSourceDict


makeHashMapFromMap :: Map ByteString ByteString -> HashMap Text Text
makeHashMapFromMap = HashMap.fromList . map raise . Map.toList
  where
    raise :: (ByteString, ByteString) -> (Text, Text)
    raise (k,v) = (T.pack . S.unpack $ k, T.pack . S.unpack $ v)

filterUndesireables :: Map ByteString ByteString -> Map ByteString ByteString
filterUndesireables = Map.delete "origin"


debug :: (MonadIO m, Show s) => s -> m ()
debug = liftIO . putStrLn . show

main :: IO ()
main = do
    -- just one
    [arg] <- getArgs

    let Right name = Marquise.makeSpoolName "exporter"
    spool <- Marquise.createSpoolFile name

    Rados.runConnect (Just "vaultaire") (Rados.parseConfig "/etc/ceph/ceph.conf") $ do
        Rados.runPool "vaultaire" $ do
            let o = Origin (S.pack arg)
            let o' = Vaultaire.Origin (S.pack arg)
            let l = Contents.formObjectLabel o
            st <- Contents.readVaultObject l
            let t1 = 1393632000000000000 --  1 March
--          let t1 = 1388534400000000000 --  1 January
            let t2 = 1402790400000000000 -- 15 June
            let is = Bucket.calculateTimemarks t1 t2

            forM_ st $ \s -> do
                debug s
                -- Work out address
                let a = hashSourceToAddress o s

                -- All tags, less undesirables
                let s' = convertSourceDict s

                -- Register that source at address
                liftIO $ Marquise.withContentsConnection "localhost" $ \c ->
                    Marquise.updateSourceDict a s' o' c >>= either throw return

                -- Process all its data points
                forM_ is $ \i -> do
                    liftIO $ putStr $ (show o) ++ " " ++ (show a) ++ " " ++ (show i) ++ " "
                    m <- Bucket.readVaultObject o s i

                    if (Map.null m)
                      then do
                        liftIO $ printf "%6s\n" ("-" :: String)
                      else do
                        let ps = Bucket.pointsInRange t1 t2 m
                        liftIO $ printf "%6d\n" (length ps)
                        liftIO $ forM_ ps (convertPointAndWrite spool a)


convertPointAndWrite :: Marquise.SpoolFile -> Marquise.Address -> Point -> IO ()
convertPointAndWrite spool a p =
  let
    t = Marquise.TimeStamp (timestamp p)
  in
    case payload p of
        Empty           -> Marquise.sendSimple   spool a t 0
        Numeric n       -> Marquise.sendSimple   spool a t (fromIntegral n)
        Measurement r   -> Marquise.sendSimple   spool a t (doubleToWord r)
        Textual s       -> Marquise.sendExtended spool a t (T.encodeUtf8 s)     -- DANGER partial
        Blob b          -> Marquise.sendExtended spool a t b
