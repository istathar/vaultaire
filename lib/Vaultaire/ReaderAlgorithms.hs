{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Vaultaire.ReaderAlgorithms
(
    deDuplicate,
    Point(..),
    processBucket,
    mergeSimpleExtended,
    mergeNoFilter,
    similar,
    deDuplicateLast,
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Primitive
import Control.Monad.ST (runST)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.ByteString.Lazy.Builder
import Data.Monoid
import Data.Packer
import qualified Data.Vector.Algorithms.Merge as M
import Data.Vector.Generic.Mutable (MVector)
import qualified Data.Vector.Generic.Mutable as M
import qualified Data.Vector.Storable as V
import Data.Vector.Storable.ByteString
import Data.Word
import Foreign.Ptr
import Foreign.Storable
import Pipes
import Prelude hiding (filter)

import Vaultaire.CoreTypes (Address (..))

data Point = Point { address :: !Word64
                   , time    :: !Word64
                   , payload :: !Word64
                   } deriving (Show, Eq)

instance Storable Point where
    sizeOf _    = 24
    alignment _ = 8
    peek ptr =
        Point <$> peek (castPtr ptr)
              <*> peek (ptr `plusPtr` 8)
              <*> peek (ptr `plusPtr` 16)
    poke ptr (Point a t p) =  do
        poke (castPtr ptr) a
        poke (ptr `plusPtr` 8 ) t
        poke (ptr `plusPtr` 16 ) p

instance Ord Point where
    -- Compare time first, then address. This way we can de-deplicate by
    -- comparing adjacent values.
    compare a b =
        case compare (time a) (time b) of
            EQ -> compare (address a) (address b)
            c  -> c

-- | Is the address and time the same? We don't care about the payload
similar :: Point -> Point -> Bool
similar a b = (address a == address b) && (time a == time b)

-- | Sort and de-duplicate elements. First element wins.
deDuplicate :: (PrimMonad m, MVector v e, Ord e)
            => (e -> e -> Bool)
            -> v (PrimState m) e
            -> m (v (PrimState m) e)
deDuplicate cmp input
    | M.null input = return input
    | otherwise = do
        first <- M.unsafeRead input 0
        go input first 1 1 (M.length input)
  where
    go buf prev_elt read_ptr write_ptr len
        | read_ptr == len = return $ M.take write_ptr buf
        | otherwise = do
            elt <- M.unsafeRead buf read_ptr

            if elt `cmp` prev_elt
                then
                    go buf prev_elt (succ read_ptr) write_ptr len
                else do
                    -- This conditional is an optimization for non-duplicate
                    -- data.
                    when (write_ptr /= read_ptr) $
                        M.unsafeWrite buf write_ptr elt

                    go buf elt (succ read_ptr) (succ write_ptr) len

--
-- | Sort and de-duplicate elements. Last element wins.
deDuplicateLast :: (PrimMonad m, MVector v e, Ord e, Eq e)
                => (e -> e -> Bool)
                -> v (PrimState m) e
                -> m (v (PrimState m) e)
deDuplicateLast cmp input
    | M.null input = return input
    | otherwise = do
        first <- M.unsafeRead input 0
        go input first 1 0 (M.length input)
  where
    go buf prev_elt read_ptr write_ptr len
        | read_ptr == len = do
            -- Copy last element, it's always not-duplicate or the last in a
            -- duplicate block.
            M.unsafeRead buf (pred len) >>= M.unsafeWrite buf write_ptr
            return $ M.take (succ write_ptr) buf
        | otherwise = do
            elt <- M.unsafeRead buf read_ptr

            -- Skip duplicates, reading ahead by one. Skip by not incrementing
            -- write pointer.
            if prev_elt `cmp` elt
                then
                    go buf elt (succ read_ptr) write_ptr len
                else do
                    M.unsafeWrite buf write_ptr prev_elt
                    go buf elt (succ read_ptr) (succ write_ptr) len


-- | Filter and de-duplicate a bucket in-place. The original bytestring will be
-- garbage after completion.
processBucket :: (PrimMonad m)
              => ByteString -> Address -> Word64 -> Word64 -> m ByteString
processBucket bucket (Address addr) start end = do
    let v  = byteStringToVector bucket
    let v' = V.filter (\p -> address p == addr && time p >= start && time p <= end) v
    mv <- V.thaw v'
    M.sort mv
    v'' <- deDuplicate similar mv >>= V.freeze
    return $ vectorToByteString v''

-- | Merge a simple and extended bucket into one bytestring, suitable for wire
-- transfer.
mergeSimpleExtended :: (PrimMonad m)
                    => ByteString -> ByteString
                    -> Address -> Word64 -> Word64
                    -> m ByteString
mergeSimpleExtended simple extended addr start end = do
    de_duped <- byteStringToVector `liftM` processBucket simple addr start end
    return $ toStrict $ toLazyByteString $ V.foldl' merge mempty de_duped
  where
    merge acc (Point addr' time' os) =
        let bytes = runUnpacking (getExtendedBytes os) extended
            bldr = word64LE addr' <> word64LE time' <> byteString bytes
        in acc <> bldr

-- | Producer for the the whole bucket, no filtering, returns only addresses
-- and payloads. This is used for the internal store, where last writes win.
mergeNoFilter :: (Monad m)
              => ByteString -> ByteString
              -> Producer (Address, ByteString) m ()
mergeNoFilter simple extended = do
    let de_duped = runST $ preProcess simple
    V.forM_ de_duped $ \(Point addr _ os) ->
        let bytes = runUnpacking (getExtendedPayloadOnly os) extended
        in  yield (Address addr, bytes)
  where
    preProcess bs = V.thaw (byteStringToVector bs :: V.Vector Point)
                    >>= (\v -> M.sort v
                    >>  deDuplicateLast similar v)
                    >>= V.freeze

-- First word is the length, then the string. We return the length and the
-- string as a string.
getExtendedBytes :: Word64 -> Unpacking ByteString
getExtendedBytes offset = do
    unpackSetPosition (fromIntegral offset)
    len <- getWord64LE
    unpackSetPosition (fromIntegral offset)
    getBytes (fromIntegral len + 8)

-- First word is the length, then the string. We return just the string.
getExtendedPayloadOnly :: Word64 -> Unpacking ByteString
getExtendedPayloadOnly offset = do
    unpackSetPosition (fromIntegral offset)
    len <- getWord64LE
    getBytes (fromIntegral len)
