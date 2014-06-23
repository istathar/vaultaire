--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marquise.IO.Writer
(
) where

import Control.Exception
import Marquise.Classes
import Marquise.IO.Connection
import Vaultaire.Types

instance MarquiseWriterMonad IO where
    transmitBytes broker origin bytes =
        withConnection ("tcp://" ++ broker ++ ":5560") $ \c -> do
            send (PassThrough bytes) origin c
            result <- recv c
            case result of
                Left e -> throw e
                Right OnDisk -> return ()
                Right InvalidWriteOrigin -> error "send: Invalid origin"
