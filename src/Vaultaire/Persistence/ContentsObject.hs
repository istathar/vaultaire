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

{-# LANGUAGE InstanceSigs      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-orphans #-}

module Vaultaire.Persistence.ContentsObject (
    formObjectLabel
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Locator
import Data.Map.Strict (Map)
import Data.Serialize
import Data.Text (Text)
import Data.Word

import qualified Vaultaire.Internal.CoreTypes as Core
import Vaultaire.Persistence.Constants


--
-- For each origin, we maintain a list of known sources. This is the name of
-- the object we store it in
--
formObjectLabel :: Core.Contents -> S.ByteString
formObjectLabel c =
    S.intercalate "_" [__EPOCH__, o', __CONTENTS__]
  where
    o' = Core.locator c

