{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where
import Prelude hiding (words)

import Control.Concurrent.MVar
import Criterion.Main
import Data.Bits
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.List.NonEmpty (fromList)
import Data.Monoid
import Data.Word (Word64)
import System.Rados.Monadic
import System.ZMQ4.Monadic
import TestHelpers (runTestDaemon, runTestPool)
import Vaultaire.Broker
import Vaultaire.Util
import Vaultaire.Writer

createDays :: Word64 -> Word64 -> IO ()
createDays simple_buckets ext_buckets = runTestPool $ do
    _ <- runObject "02_PONY_simple_days" $
        writeFull (makeDayFile simple_buckets)
    _ <- runObject "02_PONY_extended_days" $
        writeFull (makeDayFile ext_buckets)
    return ()

makeDayFile :: Word64 -> ByteString
makeDayFile n = toStrict $ toLazyByteString b
  where
    b = word64LE 0 <> word64LE (n * 2)

runTest :: ByteString -> IO [ByteString]
runTest msg =
    runZMQ $ do
        s <- socket Dealer
        connect s "tcp://localhost:5560"
        sendMulti s $ fromList ["\x42", "PONY", msg]
        receiveMulti s

simplePoints :: [Word64] -> ByteString
simplePoints = toStrict . toLazyByteString . mconcat . map makeSimplePoint

makeSimplePoint :: Word64 -> Builder
makeSimplePoint n =
    word64LE ((n `mod` uniqueAddresses) `clearBit` 0) -- address
    <> word64LE n                                     -- time
    <> word64LE n                                     -- payload
  where
    uniqueAddresses = 1000 * 2

main :: IO ()
main = do
    runTestDaemon "tcp://localhost:1234" (return ())
    createDays 32 32

    linkThread $ runZMQ $ startProxy
        (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"

    quit <- newEmptyMVar
    linkThread $ startWriter "tcp://localhost:5561" Nothing "test" 0 quit

    let !points = simplePoints [0..10000]

    defaultMain
            [ bench "10000 simple points over 1000 addresses" $
                nfIO $ runTest points
            ]
