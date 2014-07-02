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

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Main where

import qualified Control.Concurrent.Async as A
import Control.Monad ((>=>))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Binary.IEEE754 (doubleToWord)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Foldable (foldlM, forM_)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time (UTCTime, formatTime)
import Data.Time.Clock.POSIX
import System.Environment (getArgs)
import System.IO (Handle, IOMode (..), hFlush, openFile)
import System.Locale (defaultTimeLocale)
import qualified System.Rados.Monadic as Rados
import Text.Printf

import Version1.Internal.CoreTypes
import qualified Version1.Persistence.BucketObject as Bucket
import qualified Version1.Persistence.ContentsObject as Contents

import qualified "marquise" Marquise.Client as Marquise
import qualified "vaultaire-common" Vaultaire.Types as Vaultaire

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
    raise (k,v) = (T.pack . S.unpack . clean $ k, T.pack . S.unpack . clean $ v)

    clean = S.map (\c -> if c == ':' || c == ',' then '-' else c)

filterUndesireables :: Map ByteString ByteString -> Map ByteString ByteString
filterUndesireables = Map.delete "origin"


debug :: (MonadIO m, Show s) => s -> m ()
debug = liftIO . putStrLn . show

report :: MonadIO m => Handle -> Origin -> Timemark -> Int -> m ()
report h o i n = liftIO $ do
    let date = formatTimestamp (posixSecondsToUTCTime (fromIntegral i))
    hPrintf h "%s %s %d %6d\n" date (show o) i n
    hFlush h


formatTimestamp :: UTCTime -> String
formatTimestamp x = formatTime defaultTimeLocale "%e %b %y, %H:%M:%S" x


withPool :: Rados.Pool a -> IO a
withPool action = Rados.runConnect (Just "vaultaire") (Rados.parseConfig "/etc/ceph/ceph.conf")
                    (Rados.runPool "vaultaire" action)

forkThread :: IO a -> IO ()
forkThread = A.async >=> A.link

processSource sf o i !n s = do
                -- Work out address
                let a' = hashSourceToAddress o s

                m <- Bucket.readVaultObject o s i

                if (Map.null m)
                  then do
                    return n
                  else do
{-
                    let ps = Bucket.pointsInRange t1 t2 m
-}
                    let n2 = Map.size m
                    liftIO $ printf "%s %s %d %6d\n" (show o) (show a') i n2
                    liftIO $ forM_ m (convertPointAndWrite sf a')
                    return (n + n2)


main :: IO ()
main = do
    [arg1, arg2, arg3] <- getArgs

    let t1 = read (arg2 ++ "000000000")
    let t2 = read (arg3 ++ "000000000")


--  sfs <- replicateM 4 (Marquise.createSpoolFiles "exporter")
    sf <- Marquise.createSpoolFiles "exporter"

    let is = Bucket.calculateTimemarks t1 t2
    let o  = Origin (S.pack arg1)

    putStrLn "-- load contents list"

    st <- withPool $ do
            let l = Contents.formObjectLabel o
            Contents.readVaultObject l

    putStrLn "-- convert data points"

    h <- openFile "exporter.log" WriteMode


    withPool $ do
        forM_ is $ \i -> do
            count <- foldlM (processSource sf o i) 0 st
            report h o i count


    putStrLn "-- convert contents"

    withPool $ do
            forM_ st $ \s -> do
                debug s
                -- Work out address
                let a' = hashSourceToAddress o s

                -- All tags, less undesirables
                let s' = convertSourceDict s

                -- Register that source at address
                liftIO $ Marquise.queueSourceDictUpdate sf a' s'


convertPointAndWrite :: Marquise.SpoolFiles -> Marquise.Address -> Point -> IO ()
convertPointAndWrite spool a p =
  let
    t = Marquise.TimeStamp (timestamp p)
  in
    case payload p of
        Empty           -> Marquise.queueSimple   spool a t 0
        Numeric n       -> Marquise.queueSimple   spool a t (fromIntegral n)
        Measurement r   -> Marquise.queueSimple   spool a t (doubleToWord r)
        Textual s       -> Marquise.queueExtended spool a t (T.encodeUtf8 s)     -- DANGER partial
        Blob b          -> Marquise.queueExtended spool a t b

