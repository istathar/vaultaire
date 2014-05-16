{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Vaultaire.ReaderAlgorithms
(
    filter,
    deDuplicate,
    Point(..),
    processBucket,
    mergeSimpleExtended,
    mergeNoFilter,
    deDuplicateLast,
) where

import Debug.Trace

import Control.Applicative
import Control.Monad
import Control.Monad.Primitive
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Control.Monad.ST(runST)
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
                   } deriving Show

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

instance Eq Point where
    -- Two Points are effectively equal if their payloads are the same as they
    -- are to be de-duplicated.
    a == b = time a == time b &&
             address a == address b

instance Ord Point where
    -- Compare time first, then address. This way we can de-deplicate by
    -- comparing adjacent values.
    compare a b =
        case compare (time a) (time b) of
            EQ -> compare (address a) (address b)
            c  -> c

-- | Filter a vector of Points based on:
filter :: (PrimMonad m, MVector v Point)
       => Word64 -- ^ Address
       -> Word64 -- ^ Start
       -> Word64 -- ^ End
       -> v (PrimState m) Point
       -> m (v (PrimState m) Point)
filter addr start end input =
    go input 0 0 $ M.length input
 where
    go buf read_ptr write_ptr len
        | read_ptr == len = return $ M.take write_ptr buf
        | otherwise = do
            p@(Point a t _) <- M.unsafeRead buf read_ptr
            if a == addr && t >= start && t <= end
                then do
                    M.unsafeWrite buf write_ptr p
                    go buf (succ read_ptr) (succ write_ptr) len
                else
                    go buf (succ read_ptr) write_ptr len


-- | Sort and de-duplicate elements. First element wins.
deDuplicate :: (PrimMonad m, MVector v e, Ord e, Eq e) => v (PrimState m) e -> m (v (PrimState m) e)
deDuplicate input
    | M.null input = return input
    | otherwise = do
        M.sort input
        first <- M.unsafeRead input 0
        go input first 1 1 (M.length input)
  where
    go buf prev_elt read_ptr write_ptr len
        | read_ptr == len = return $ M.take write_ptr buf
        | otherwise = do
            elt <- M.unsafeRead buf read_ptr

            if elt == prev_elt
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
deDuplicateLast :: (PrimMonad m, MVector v e, Ord e, Eq e, Show e) => v (PrimState m) e -> m (v (PrimState m) e)
deDuplicateLast input
    | M.null input = return input
    | otherwise = do
        M.sort input
        first <- M.unsafeRead input 0
        go input first 1 1 (M.length input)
  where
    go buf prev_elt read_ptr write_ptr len
        | read_ptr == len = return $ M.take write_ptr buf
        | otherwise = do
            elt <- M.unsafeRead buf read_ptr

            -- Only advance the write pointer on non-duplicate data
            let next_write = if elt == prev_elt
                                then trace "equal, not advancing" $ write_ptr
                                else trace "not equal, advancing" $ succ write_ptr

            when (write_ptr /= read_ptr) $
                M.unsafeWrite buf write_ptr (traceShow ("writing", write_ptr, elt) elt)

            go buf elt (succ read_ptr) next_write len

-- | Filter and de-duplicate a bucket in-place. The original bytestring will be
-- garbage after completion.
processBucket :: (PrimMonad m, Functor m)
              => ByteString -> Address -> Word64 -> Word64 -> m ByteString
processBucket bucket (Address addr) start end =
    vectorToByteString <$> (V.thaw (byteStringToVector bucket)
                            >>= filter addr start end
                            >>= deDuplicate
                            >>= V.freeze)

-- | Merge a simple and extended bucket into one bytestring, suitable for wire
-- transfer.
mergeSimpleExtended :: (PrimMonad m, Functor m)
                    => ByteString -> ByteString
                    -> Address -> Word64 -> Word64
                    -> m ByteString
mergeSimpleExtended simple extended addr start end = do
    de_duped <- byteStringToVector <$> processBucket simple addr start end
    return $ toStrict $ toLazyByteString $ V.foldl' merge mempty de_duped
  where
    merge acc (Point addr' time' os) =
        let bytes = runUnpacking (getExtendedBytes os) extended
            bldr = word64LE addr' <> word64LE time' <> byteString bytes
        in acc <> bldr

-- | Producer for the the whole bucket, no filtering, returns only addresses
-- and payloads. This is used for the internal store, where last writes win.
mergeNoFilter :: (Monad m, Functor m)
              => ByteString -> ByteString
              -> Producer (Address, ByteString) m ()
mergeNoFilter simple extended = do
    let de_duped = runST $ preProcess simple
    V.forM_ de_duped $ \(Point addr _ os) ->
        let bytes = runUnpacking (getExtendedPayloadOnly os) extended
        in  yield (Address addr, bytes)
  where
    preProcess bs = V.thaw (byteStringToVector bs :: V.Vector Point)
                    >>= deDuplicateLast
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
