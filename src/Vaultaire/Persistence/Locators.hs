--
-- Data vault for metrics
--
-- Copyright © 2011-2014 Operational Dynamics Consulting, Pty Ltd
-- Copyright © 2014-     Anchor Systems, Pty Ltd
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--
-- This code originally licenced GPLv2. Relicenced BSD3 on 2 Jan 2014.
--

{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Persistence.Locators
(
    fromLocator16,
    toLocator16,
    toLocator16a,
    hashStringToLocator16a
) where


import Prelude hiding (toInteger)

import Crypto.Hash.SHA1 as Crypto
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.List (mapAccumL)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word
import Numeric (showIntAtBase)

{-

    ['A','J','K','8']
    ['B','C','D','E','G','P','T','V', 'Z' '3']
    -- 'Z' because Americans can't pronounce Zed properly.
    ['F','S']
    -- don't use 'S', conflicts '5' in handwriting.
    ['H']
    ['I','Y','5']
    ['L']
    -- suspect; scores low in readback tests
    ['M', 'N']
    ['O', '0']
    -- can't tell the difference between 'O' and '0', and meanwhile people screw up 'Q' all the time.
    ['Q','U','W','2']
    ['R']
    ['X','6']
    ['1']
    -- conflicts with lower case 'l' and upper case 'I'
    ['4']
    ['7']
    -- looks like '1' to Europeans
    ['9']
-}


--
-- Conversion between decimal and locator 16
--

data Locator
    = Zero      --  0
    | One       --  1
    | Two       --  2
    | Charlie   --  3
    | Four      --  4
    | Foxtrot   --  5
    | Hotel     --  6
    | Seven     --  7
    | Eight     --  8
    | Nine      --  9
    | Kilo      -- 10
    | Lima      -- 11
    | Mike      -- 12
    | Romeo     -- 13
    | XRay      -- 14
    | Yankee    -- 15
    deriving (Eq, Ord, Enum, Bounded)


instance Show Locator where
    show x = [c]
      where
        c = locatorToDigit x


represent :: Int -> Char
represent n =
    locatorToDigit $ toEnum n


locatorToDigit :: Locator -> Char
locatorToDigit x =
    case x of
        Zero    -> '0'
        One     -> '1'
        Two     -> '2'
        Charlie -> 'C'
        Four    -> '4'
        Foxtrot -> 'F'
        Hotel   -> 'H'
        Seven   -> '7'
        Eight   -> '8'
        Nine    -> '9'
        Kilo    -> 'K'
        Lima    -> 'L'
        Mike    -> 'M'
        Romeo   -> 'R'
        XRay    -> 'X'
        Yankee  -> 'Y'


value :: Char -> Int
value c =
    fromEnum $ digitToLocator c


digitToLocator :: Char -> Locator
digitToLocator c =
    case c of
        '0' -> Zero
        '1' -> One
        '2' -> Two
        'C' -> Charlie
        '4' -> Four
        'F' -> Foxtrot
        'H' -> Hotel
        '7' -> Seven
        '8' -> Eight
        '9' -> Nine
        'K' -> Kilo
        'L' -> Lima
        'M' -> Mike
        'R' -> Romeo
        'X' -> XRay
        'Y' -> Yankee
        _   -> error "Illegal digit"


toLocator16 :: Int -> String
toLocator16 x =
    showIntAtBase 16 represent x ""


toLocator16a :: Int -> String
toLocator16a n =
  let
    ls = convert n []
    (_,us) = mapAccumL uniq Set.empty ls
  in
    map locatorToDigit us
  where
    convert :: Int -> [Locator] -> [Locator]
    convert 0 xs = xs
    convert i xs =
      let
        (d,r) = divMod i 16
        x = toEnum r
      in
        convert d (x:xs)

    uniq :: Set Locator -> Locator -> (Set Locator, Locator)
    uniq s x =
        if Set.member x s
            then uniq s (subsequent x)
            else (Set.insert x s, x)

    subsequent :: Locator -> Locator
    subsequent x =
        if x == maxBound
            then minBound
            else succ x


padWithZeros :: Int -> Int -> String
padWithZeros digits x =
    pad ++ str
  where
    pad = take len (replicate digits '0')
    len = digits - length str
    str = toLocator16a x


multiply :: Int -> Char -> Int
multiply acc c =
    acc * 16 + value c

fromLocator16 :: String -> Int
fromLocator16 ss =
    foldl multiply 0 ss


--
-- Given a URL, convert it into a N character hash.
--

toWords :: String -> [Word8]
toWords cs =
    map fn cs
  where
    fn :: Char -> Word8
    fn c = fromIntegral $ fromEnum c

concatToInteger :: [Word8] -> Int
concatToInteger bytes =
    foldl fn 0 bytes
  where
    fn acc b = (acc * 256) + (fromIntegral b)

digest :: String -> Int
digest ws =
    i
  where
    i  = concatToInteger h
    h  = B.unpack h'
    h' = Crypto.hash x'
    x' = S.pack ws


--
-- | Take an arbitrary string, hash it, then padWithZeros it as a short
-- @digits@-long locator16 string.
--
hashStringToLocator16a :: Int -> S.ByteString -> S.ByteString
hashStringToLocator16a digits s' =
    r'
  where
    s = S.unpack s'
    n  = digest s               -- SHA1 hash
    limit = 16 ^ digits
    x  = mod n limit            -- trim to specified number locator10 chars
    r  = padWithZeros digits x  -- convert to String
    r' = S.pack r

