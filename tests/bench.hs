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

{-
import Prelude hiding (foldl)
-}

import Criterion.Main
import GHC.Conc
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map.Strict as Map
import Data.Hashable (Hashable)
{-
import Data.Foldable

benchOrdMap :: (Foldable α, Hashable α, Ord α) => α -> Pure
-}
benchOrdMap :: (Hashable α, Ord α) => α -> Pure
benchOrdMap as =
  let
    m xs = foldl (\x m -> Map.insert x x m) Map.empty xs
  in
    whnf m as

benchHashMap :: (Hashable α, Ord α) => α -> Pure
benchHashMap _ = undefined

x = [1..1000] :: [Int]

main :: IO ()
main = do
    setNumCapabilities 4
    defaultMain
       [bench "Data.Map.Strict" (benchOrdMap x),
        bench "Data.HashMap.Strict | sort" (benchHashMap x)]
    putStrLn "Complete."

