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
{-# OPTIONS -fno-warn-orphans #-}

module Vaultaire.Persistence.Constants where

import Data.ByteString (ByteString)

--
-- Epoch version of the bucket object labels. This is only a sanity guard.
-- Different object label versions must be in different pools, as there is (by
-- design) no logic to probe for differnt epoch versions; the code reading a
-- given pool should know the one [and only one] epoch it is valid for.
--

__EPOCH__ :: ByteString
__EPOCH__ = "01"

--
-- Number of seconds per bucket.
--

__WINDOW_SIZE__ :: Int
__WINDOW_SIZE__ = 100000


--
-- static name of contents buckets
--

__CONTENTS__ :: ByteString
__CONTENTS__ = "contents"


