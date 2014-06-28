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

{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE InstanceSigs      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports    #-}
{-# OPTIONS -fno-warn-type-defaults #-}

module Vaultaire.Persistence.BucketObject (
    formObjectLabel,
    floorTimestampToMark,
    calculateTimemarks,
    pointsInRange,
    appendVaultPoints,
    readVaultObject,

    -- for testing
    hashOriginName,
    tidyOriginName
) where

import Blaze.ByteString.Builder
import Control.Exception
import "mtl" Control.Monad.Error ()
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Char
import Data.Locator
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Serialize
import Data.Word
import System.Rados.Monadic

import Vaultaire.Conversion.Reader
import Vaultaire.Internal.CoreTypes
import Vaultaire.Persistence.Constants
import qualified Vaultaire.Serialize.DiskFormat as Disk

{-
    I'd really like to think there's an easier way of doing constants
-}

windowSize :: Word64
windowSize = fromIntegral __WINDOW_SIZE__

--
-- Use the relevant information from a point to find out what bucket
-- it belongs in.
--
formObjectLabel :: Origin -> ByteString -> Timestamp -> Label
formObjectLabel o s' t =
    Label l'
  where
    l' = S.intercalate "_" [__EPOCH__, o', s', i']
    (Origin o') = o
    i' = S.pack $ show $ floorTimestampToMark t

formObjectLabel2 :: Origin -> ByteString -> Timemark -> Label
formObjectLabel2 o s' i =
    Label l'
  where
    l' = S.intercalate "_" [__EPOCH__, o', s', i']
    (Origin o') = o
    i' = S.pack $ show i

--
-- Convert a Word64 timestamp in nanoseconds to a number in seconds rounded to
-- the nearest of our "metric day" windows. Have to use Integral to beat the
-- Y2038 problem.
--
floorTimestampToMark :: Timestamp -> Timemark
floorTimestampToMark t =
  let
    day = t `div` (windowSize * nanoseconds)
    sec = day * windowSize
  in
    fromIntegral sec


calculateTimemarks :: Timestamp -> Timestamp -> [Timemark]
calculateTimemarks t1 t2 =
    -- FIXME just do the math manually in a loop. Using Enum silly
    enumFromThenTo t1floor (t1floor + __WINDOW_SIZE__) t2ceiling
  where
    t1a = if t2 > t1
            then t1
            else t2
    t2a = if t2 > t1
            then t2
            else t1
    t1floor   = floorTimestampToMark t1a
    t2ceiling = floorTimestampToMark t2a


--
-- This is a bit pedantic. OrdMap provides a split function, but it doesn't
-- include the lookup value if it is present. Which is a case we'll hit a lot,
-- so we use splitLookup instead...  which forces us to deal with it manually.
-- Oh well.
--
pointsInRange :: Timestamp -> Timestamp -> Map Timestamp Point -> [Point]
pointsInRange t1 t2 m =
  let
    (_, a', middle) = Map.splitLookup t1 m
    (valid, b', _)  = Map.splitLookup t2 middle
    ps1 = Map.elems valid
    ps2 = case a' of
            Just a  -> a:ps1
            Nothing -> ps1
    ps3 = case b' of
            Just b  -> ps2 ++ [b]        -- BAD FIXME BAD!
            Nothing -> ps2
  in
    ps3


tidyOriginName :: ByteString -> ByteString
tidyOriginName o' =
  let
    width = 10

    predicate :: Char -> Bool
    predicate c = isAscii c && isPrint c && (c /= '_')

    n' = S.append (S.filter predicate o') (S.replicate width ':')
  in
    S.take width n'


hashOriginName :: ByteString -> ByteString
hashOriginName o' =
    hashStringToLocator16a 6 o'





--
-- | Given a list of actions @as@, feed them @limit@ at a time into a second
-- function @f@ (in that same monad) that evaluates them.
--

onlySoManyAtATime_ :: Monad ω => Int -> [ω α] -> (α -> ω ()) -> ω ()
onlySoManyAtATime_ _ [] _ = return ()
onlySoManyAtATime_ limit actions f =
  let
    (prefix, remainder) = splitAt limit actions
  in do
    carryOut prefix f
    onlySoManyAtATime_ limit remainder f

  where
    carryOut :: Monad ω => [ω α] -> (α -> ω ()) -> ω ()
    carryOut as g = do
        xs <- sequence as
        mapM_ g xs


--
-- The origin contents file is locked before entering here. Build a map of
-- labels to encoded points, then construct a list of asynchronous appends.
--
appendVaultPoints :: Int -> Map Label Builder -> Pool ()
appendVaultPoints limit m0 =
    onlySoManyAtATime_ limit (action m0) checkError
  where
    action m = Map.foldrWithKey asyncAppend [] m :: [Pool AsyncWrite]

    asyncAppend (Label l') bB as =
        (runAsync . runObject l' $ append $ toByteString bB) : as

    checkError write_in_flight = do
        maybe_error <- waitComplete write_in_flight
        case maybe_error of
            Just err    -> liftIO $ throwIO err
            Nothing     -> return ()

{-
    This whole thing is a bit crazy. We should just merge it all into a single
    use of Data.Serialize.Get
-}

readVaultObject
    :: Origin
    -> SourceDict
    -> Timemark
    -> Pool (Map Timestamp Point)
readVaultObject o s i =
  let
    s' = hashSourceDict s           -- FIXME lookup from Directory
    l  = formObjectLabel2 o s' i
    Label l' = l

  in do
    ey' <- runObject l' readFull    -- Pool (Either RadosError ByteString)

    case ey' of
        Left (NoEntity _ _ _)   -> return Map.empty
        Left err                -> liftIO $ throwIO err
        Right y'                -> either error return $ process y' Map.empty

  where

--
-- First write wins. This is a crucial design property; we DO expect duplicate
-- writes as a consequence of the distributed system design of Vaultaire:
-- points are idempotent for a given timestamp; if we see another no need to
-- insert it. This also has an important security aspect: someone can
-- maliciously write later, but we will ignore it and thereby not destroy data.
--

        process :: ByteString -> Map Timestamp Point -> Either String (Map Timestamp Point)
        process y' !m1 =
            if S.null y'
                then return m1
                else do
                    (p,z') <- readPoint2 y'
                    let k = timestamp p
                    let m2 = if Map.member k m1
                            then m1
                            else Map.insert k p m1

                    process z' m2


        readPoint2 :: ByteString -> Either String (Point, ByteString)
        readPoint2 x' = do
            ((VaultRecord _ pb), remainder') <- runGetState get x' 0
            return (convertVaultToPoint o s pb, remainder')


data VaultRecord = VaultRecord Disk.VaultPrefix Disk.VaultPoint

instance Serialize VaultRecord where
    put (VaultRecord prefix point) = do
        put prefix
        put point

    get = do
        prefix <- get
        let len = fromIntegral $ Disk.size prefix
        point <- isolate len get
        return $ VaultRecord prefix point



