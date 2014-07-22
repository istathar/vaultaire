--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE RecordWildCards #-}

module CommandRunners
(
    runReadPoints,
    runListContents,
    runDumpDayMap,
    runRegisterOrigin
) where

import Control.Concurrent.MVar
import Control.Exception (throw)
import Control.Monad
import qualified Data.ByteString.Char8 as S
import Data.Map (fromAscList)
import Data.Word (Word64)
import Marquise.Client
import Pipes
import System.Log.Handler.Syslog
import System.Log.Logger
import System.Rados.Monadic (RadosError (..), runObject, stat, writeFull)
import Vaultaire.Daemon (dayMapsFromCeph, extendedDayOID, simpleDayOID,
                         withPool)
import Vaultaire.Types


runReadPoints :: String -> Origin -> Address -> Word64 -> Word64 -> MVar () -> IO ()
runReadPoints broker origin addr start end shutdown = do
    withReaderConnection broker $ \c ->
        runEffect $ for (readSimple addr start end origin c >-> decodeSimple)
                        (lift . print)
    putMVar shutdown ()

runListContents :: String -> Origin -> MVar () -> IO ()
runListContents broker origin shutdown = do
    withContentsConnection broker $ \c ->
        runEffect $ for (enumerateOrigin origin c) (lift . print)
    putMVar shutdown ()

runDumpDayMap :: String -> String -> Origin -> MVar () -> IO ()
runDumpDayMap pool user origin shutdown =  do
    let user' = Just (S.pack user)
    let pool' = S.pack pool

    maps <- withPool user' pool' (dayMapsFromCeph origin)
    case maps of
        Left e -> error e
        Right ((_, simple), (_, extended)) -> do
            putStrLn "Simple day map:"
            print simple
            putStrLn "Extended day map:"
            print extended
    putMVar shutdown ()

runRegisterOrigin :: String -> String -> Origin -> Word64 -> Word64 -> Word64 -> Word64 -> MVar () -> IO ()
runRegisterOrigin pool user origin buckets step begin end shutdown = do
    let targets = [simpleDayOID origin, extendedDayOID origin]
    let user' = Just (S.pack user)
    let pool' = S.pack pool

    withPool user' pool' (forM_ targets initializeDayMap)
    putMVar shutdown ()
  where
    initializeDayMap target =
        runObject target $ do
            result <- stat
            case result of
                Left NoEntity{} -> return ()
                Left e -> throw e
                Right _ -> error "Origin already registered."

            writeFull (toWire dayMap) >>= maybe (return ()) throw

    dayMap = DayMap . fromAscList $
        ((0, buckets):)[(n, buckets) | n <- [begin,begin+step..end]]
