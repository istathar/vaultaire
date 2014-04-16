{-# LANGUAGE OverloadedStrings #-}

module TestHelpers
(
    cleanup,
    loadState,
    writePonyDayMap,
    throwJust,
    dayFileA,
    dayFileB,
    dayFileC,
    dayFileD,
    runTestDaemon,
    runTestPool,
    prettyPrint
)
where

import Vaultaire.Daemon
import Vaultaire.RollOver
import System.Rados.Monadic
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Numeric(showHex)

cleanup :: Daemon ()
cleanup = liftPool $ unsafeObjects >>= mapM_ (`runObject` remove)

loadState :: Daemon ()
loadState = do
    writePonyDayMap "02_PONY_simple_days" dayFileA
    writePonyDayMap "02_PONY_extended_days" dayFileB
    refreshOriginDays "PONY"
    updateSimpleLatest "PONY" 0x42
    updateExtendedLatest "PONY" 0x52


dayFileA, dayFileB, dayFileC, dayFileD:: ByteString
dayFileA = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x08\x00\x00\x00\x00\x00\x00\x00"

dayFileB = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x0f\x00\x00\x00\x00\x00\x00\x00"

dayFileC = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x0f\x00\x00\x00\x00\x00\x00\x00\
           \\xff\x00\x00\x00\x00\x00\x00\x00\
           \\xfe\x00\x00\x00\x00\x00\x00\x00"

dayFileD = "\x00\x00\x00\x00\x00\x00\x00\x00\
           \\x08\x00\x00\x00\x00\x00\x00\x00\
           \\x42\x00\x00\x00\x00\x00\x00\x00\
           \\x08\x00\x00\x00\x00\x00\x00\x00"

writePonyDayMap :: ByteString -> ByteString -> Daemon ()
writePonyDayMap oid contents = liftPool $ 
    runObject oid (writeFull contents)
    >>= throwJust

throwJust :: Monad m => Maybe RadosError -> m ()
throwJust =
    maybe (return ()) (error . show)
 
runTestDaemon :: String -> Daemon a -> IO a
runTestDaemon broker a =
    runDaemon broker Nothing "test" (cleanup >> loadState >> a)

runTestPool :: Pool a -> IO a
runTestPool = runConnect Nothing (parseConfig "/etc/ceph/ceph.conf")
              . runPool "test"

prettyPrint :: ByteString -> String
prettyPrint = concatMap (`showHex` "") . BS.unpack

