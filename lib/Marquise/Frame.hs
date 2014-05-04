{-# LANGUAGE OverloadedStrings #-}

module Marquise.Frame where

import Data.Int (Int64)

data Frame = Frame {
    Address :: !Int64,
    Timestamp :: !Int64,
    Data :: !Int64
}


