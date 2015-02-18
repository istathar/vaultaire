{-# LANGUAGE OverloadedStrings #-}
-- | Day file related rollover actions. Daemons writing to the vault will want
-- this.
module Vaultaire.RollOver
(
    rollOverSimpleDay,
    rollOverExtendedDay,
    updateSimpleLatest,
    updateExtendedLatest,
    originLockOID,
) where

import Control.Monad.State
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Packer
import System.Rados.Monadic
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.Types

-- | Roll the cluster onto a new "vault day", this will block until all other
--   daemons are synchronized at acquiring any shared locks.
--
--   All day maps will be invalidated on roll over, it is up to you to ensure
--   that they are reloaded before next use.
rollOverSimpleDay :: Origin -> NumBuckets -> Daemon ()
rollOverSimpleDay origin' =
    rollOver origin' (simpleDayOID origin') (simpleLatestOID origin')

-- | Equivalent of 'rollOverSimpleDay' for extended buckets.
rollOverExtendedDay :: Origin -> NumBuckets -> Daemon ()
rollOverExtendedDay origin' =
    rollOver origin' (extendedDayOID origin') (extendedLatestOID origin')

-- | This compares the given time against the latest one in ceph, and updates
--   if larger.
--
--   You should only call this once with the maximum time of whatever data set
--   you are writing down. This should be done within the same lock as that
--   write.
updateSimpleLatest :: Origin -> TimeStamp -> Daemon ()
updateSimpleLatest origin' = updateLatest (simpleLatestOID origin')

-- | Equivalent of 'updateSimpleLatest' for extended buckets.
updateExtendedLatest :: Origin -> TimeStamp -> Daemon ()
updateExtendedLatest origin' = updateLatest (extendedLatestOID origin')

-- Internal

-- | Updates the latest time specified Ceph object to the provided
--   'TimeStamp', if it is later than the one the object already has.
updateLatest :: ByteString -> TimeStamp -> Daemon ()
updateLatest oid (TimeStamp time) = withLockExclusive oid . liftPool $ do
    result <- runObject oid readFull
    case result of
        Right v           -> when (parse v < time) doWrite
        Left (NoEntity{}) -> doWrite
        Left e            -> error $ show e
  where
    doWrite =
        runObject oid (writeFull value)
        >>= maybe (return ()) (error.show)
    value = runPacking 8 (putWord64LE time)
    parse = either (const 0) id . tryUnpacking getWord64LE

rollOver :: Origin -> ByteString -> ByteString -> NumBuckets -> Daemon ()
rollOver origin day_file latest_file buckets =
    withLockExclusive (originLockOID origin) $ do
        om <- get
        expired <- cacheExpired om origin
        unless expired $ do
            latest <- liftPool $ runObject latest_file readFull >>= mustLatest

            when (BS.length latest /= 8) $
                error $ "corrupt latest file in origin': " ++ show origin

            app <- liftPool . runObject day_file $
                append (latest `BS.append` build buckets)

            case app of
                Just e  -> error $ "failed to append for rollover: " ++ show e
                Nothing -> return ()
  where
    build n = runPacking 8 $ putWord64LE n
    mustLatest = either (\e -> error $ "could not get latest_file" ++ show e)
                        return

originLockOID :: Origin -> ByteString
originLockOID = simpleLatestOID

simpleLatestOID :: Origin -> ByteString
simpleLatestOID (Origin origin') =
    "02_" `BS.append` origin' `BS.append` "_simple_latest"

extendedLatestOID :: Origin -> ByteString
extendedLatestOID (Origin origin') =
    "02_" `BS.append` origin' `BS.append` "_extended_latest"

