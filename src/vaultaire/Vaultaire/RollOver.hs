{-# LANGUAGE OverloadedStrings #-}
-- | Day file related rollover actions. Daemons writing to the vault will want
-- this.
module Vaultaire.RollOver
(
    rollOverDay,
    updateLatest
) where

import Control.Monad.State
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Packer
import System.Rados.Monadic
import Vaultaire.Daemon
import Vaultaire.DayMap
import Vaultaire.OriginMap

-- | Roll the cluster onto a new "vault day", this will block until all other
-- daemons are synchronized at acquiring any shared locks.
--
-- All day maps will be invalidated on roll over, it is up to you to ensure
-- that they are reloaded before next use.
rollOverDay :: Origin -> Daemon ()
rollOverDay origin =
    let day_file = dayOID origin
        latest_file = latestOID origin
    in withExLock day_file $ do
        om <- get
        expired <- cacheExpired om origin

        unless expired $ do
            buckets <- fetchNoBuckets origin maxBound >>= mustBucket
            latest <- liftPool $ runObject latest_file readFull >>= mustLatest

            when (BS.length latest /= 8) $
                error $ "corrupt latest file in origin: " ++ show origin

            app <- liftPool . runObject day_file $
                append (latest `BS.append` build buckets)

            case app of
                Just e  -> error $ "failed to append for rollover: " ++ show e
                Nothing -> return ()
  where
    build n = runPacking 8 $ putWord64LE n
    mustBucket = maybe (error "could not find n_buckets for roll over") return
    mustLatest = either (\e -> error $ "could not get latest_file" ++ show e)
                        return

-- | This compares the given time against the latest one in ceph, and updates
-- if larger.
--
-- You should only call this once with the maximum time of whatever data set
-- you are writing down. This should be done within the same lock as that
-- write.
updateLatest :: Origin -> Time -> Daemon ()
updateLatest origin time = withExLock (latestOID origin) . liftPool $ do
    let oid = latestOID origin
    result <- runObject oid readFull
    case result of
        Right v           -> when (parse v < time) doWrite
        Left (NoEntity{}) -> doWrite
        Left e            -> error $ show e
  where
    doWrite =
        runObject (latestOID origin) (writeFull value)
        >>= maybe (return ()) (error.show)
    value = runPacking 8 (putWord64LE time)
    parse = either (const 0) id . tryUnpacking getWord64LE


-- Internal

latestOID :: Origin -> ByteString
latestOID origin = "02_" `BS.append` origin `BS.append` "_latest"

