{-# LANGUAGE BangPatterns      #-}

module Main where
import Criterion.Main
import Data.Bits
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.Monoid
import Control.Monad.ST
import Data.Word
import qualified Vaultaire.ReaderAlgorithms as A

simplePoints :: [Word64] -> ByteString
simplePoints = toStrict . toLazyByteString . mconcat . map makeSimplePoint

makeSimplePoint :: Word64 -> Builder
makeSimplePoint n =
    word64LE ((n `mod` uniqueAddresses) `clearBit` 0) -- address
    <> word64LE n                                     -- time
    <> word64LE n                                     -- payload
  where
    uniqueAddresses = 8 * 2

runTest :: ByteString -> ByteString
runTest bs = runST $ A.processBucket bs 4 minBound maxBound
    

main :: IO ()
main = do
    let !points = simplePoints [0..174763] -- 4MB
    let !double_points = simplePoints [0..349526]
    runTest double_points `seq` putStrLn "done"

    {--
    defaultMain
            [ bench "simple points" $ nf runTest points
            , bench "simple points (double)" $ nf runTest double_points
            ]
    --}
