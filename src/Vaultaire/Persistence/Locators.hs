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
    hashStringToLocator16,
    toLocator16,
    fromLocator16
) where


import Prelude hiding (toInteger)

import Crypto.Hash.SHA1 as Crypto
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.Word
import Numeric (showIntAtBase)

{-

1    ['A','J','K','8']
2    ['B','C','D','E','G','P','T','V', 'Z' '3']
    -- 'Z' because Americans can't pronounce Zed properly.
3    ['F','S']
    -- don't use 'S', conflicts '5' in handwriting.
4    ['H']
5    ['I','Y','5']
     ['L']
    -- suspect; scores low in readback tests
6    ['M', 'N']
     ['O', '0']
    -- can't tell the difference between 'O' and '0', and meanwhile people screw up 'Q' all the time.
8    ['Q','U','W','2']
9    ['R']
0    ['X','6']
     ['1']
    -- conflicts with lower case 'l' and upper case 'I'
D    ['4'] 
     ['7']
    -- disqualified, looks like '1' to Europeans
F    ['9']

-}


--
-- Conversion between decimal and base 16
--

represent :: Int -> Char
represent x =
    case x of
        0   -> '0'
        1   -> 'K'
        2   -> 'F'
        3   -> 'V'
        4   -> 'H'
        5   -> 'Y'
        6   -> 'M'
        7   -> 'R'
        8   -> 'X'
        9   -> 'U'
        10  -> '1'
        11  -> '4'
        12  -> '7'
        13  -> '8'
        14  -> '9'
        15  -> 'Z'
        _   -> error "Illegal character"


value :: Char -> Int
value c =
    case c of
        '0' -> 0
        'K' -> 1
        'F' -> 2
        'V' -> 3
        'H' -> 4
        'Y' -> 5
        'M' -> 6
        'R' -> 7
        'X' -> 8
        'U' -> 9
        '1' -> 10
        '4' -> 11
        '7' -> 12
        '8' -> 13
        '9' -> 14
        'Z' -> 15

        -- and now, some preliminary human error catching
        'o' -> 0
        'O' -> 0
        'k' -> 1
        'f' -> 2
        'v' -> 3
        'h' -> 4
        'y' -> 5
        'm' -> 6
        'r' -> 7
        'x' -> 8
        'u' -> 9
        'l' -> 10
        'i' -> 10
        'I' -> 10
        'L' -> 10
        'z' -> 15
        '2' -> 15
        _   -> error "Illegal digit"


toLocator16 :: Int -> String
toLocator16 x =
    showIntAtBase 16 represent x ""

padWithZeros :: Int -> Int -> String
padWithZeros digits x =
    pad ++ str
  where
    pad = take len (replicate digits '0')
    len = digits - length str
    str = toLocator16 x


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
hashStringToLocator16 :: Int -> S.ByteString -> S.ByteString
hashStringToLocator16 digits s' =
    r'
  where
    s = S.unpack s'
    n  = digest s               -- SHA1 hash
    limit = 16 ^ digits
    x  = mod n limit            -- trim to specified number locator10 chars
    r  = padWithZeros digits x  -- convert to String
    r' = S.pack r

