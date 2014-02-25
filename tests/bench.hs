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

{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-unused-do-bind #-}

import Prelude hiding (foldl)

import Criterion.Main
import Data.Foldable
import Data.Hashable (Hashable)
import qualified Data.HashMap.Strict as HashMap
import Data.List (sort)
import qualified Data.Map.Strict as OrdMap
import GHC.Conc


benchOrdMap :: (Foldable φ, Ord α) => φ α -> Pure
benchOrdMap as =
    whnf (g as) OrdMap.empty
  where
    f xs i = foldl (\m x -> OrdMap.insert x x m) i xs
    g xs i = OrdMap.keys $ f xs i

benchHashMap :: (Foldable φ, Eq α, Ord α, Hashable α) => φ α -> Pure
benchHashMap as =
    whnf (g as) HashMap.empty
  where
    f xs i = foldl (\m x -> HashMap.insert x x m) i xs
    g xs i = sort $ HashMap.keys $ f xs i



input = [1..1000] :: [Int]

main :: IO ()
main = do
    setNumCapabilities 4
    defaultMain
       [bench "Data.Map.Strict" (benchOrdMap input),
        bench "Data.HashMap.Strict | sort" (benchHashMap input)]
    putStrLn "Complete."

