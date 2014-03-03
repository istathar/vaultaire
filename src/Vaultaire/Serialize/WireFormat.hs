--
-- Data vault for metrics
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

module Vaultaire.Serialize.WireFormat
(
    DataFrame(..),
    SourceTag(..),
    ValueType(..),
    DataBurst(..),
    RequestSource(..),
    RequestMulti(..),
    DataSourceResponse(..),
    DataSourceResponseBurst(..)
) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import Data.Int (Int64)
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.ProtocolBuffers hiding (field)
import Data.Text (Text)
import qualified Data.Text as T
import Data.TypeLevel (D1, D2, D3, D4, D5, D6, D7, D8)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Text.Printf

import Vaultaire.Serialize.Common

data DataFrame = DataFrame {
    origin           :: Optional D8 (Value ByteString),
    source           :: Repeated D1 (Message SourceTag),
    timestamp        :: Required D2 (Value (Fixed Word64)),
    payload          :: Required D3 (Enumeration ValueType),
    valueNumeric     :: Optional D4 (Value Int64),
    valueMeasurement :: Optional D5 (Value Double),
    valueTextual     :: Optional D6 (Value Text),
    valueBlob        :: Optional D7 (Value ByteString)
} deriving (Generic, Eq)

instance Encode DataFrame
instance Decode DataFrame

instance Show DataFrame where
    show x =
        o ++ "\n" ++
        s ++ "\n" ++
        t ++ "\n" ++
        p ++ "\n" ++
        v
      where
        o = S.unpack $ fromMaybe S.empty $ getField $ origin x
        s = intercalate ",\n" $ map show (getField $ source x)
        (Fixed m) = getField $ timestamp x
        t = show m
        p = show $ getField $ payload x
        e = getField $ payload x

        v = case e of
                EMPTY   -> ""
                NUMBER  -> show $ fromMaybe 0 $ getField $ valueNumeric x
                TEXT    -> T.unpack $ fromMaybe "" $ getField $ valueTextual x
                REAL    -> show $ fromMaybe 0.0 $ getField $ valueMeasurement x
                BINARY  -> "0x" ++ (toHex $ fromMaybe S.empty $ getField $ valueBlob x)

        toHex :: ByteString -> String
        toHex = concat . map (printf "%02X") . B.unpack


data DataBurst = DataBurst {
    frames :: Repeated D1 (Message DataFrame)
} deriving (Generic, Eq, Show)

instance Encode DataBurst
instance Decode DataBurst


--
-- Query request
--

data RequestSource = RequestSource {
    requestSourceField :: Repeated D1 (Message SourceTag),
    requestAlphaField  :: Required D2 (Value (Fixed Word64)),
    requestOmegaField  :: Required D3 (Value (Fixed Word64))
} deriving (Generic, Eq, Show)

instance Encode RequestSource
instance Decode RequestSource

data RequestMulti = RequestMulti {
    multiRequestsField :: Repeated D1 (Message RequestSource)
} deriving (Generic, Eq, Show)

instance Encode RequestMulti
instance Decode RequestMulti

--
-- Contents response to chevalier
--

data DataSourceResponse = DataSourceResponse {
    responseSourceField :: Repeated D1 (Message SourceTag)
} deriving (Generic, Eq, Show)

instance Encode DataSourceResponse
instance Decode DataSourceResponse

data DataSourceResponseBurst = DataSourceResponseBurst {
    responseSourcesField :: Repeated D1 (Message DataSourceResponse),
    responseErrorField   :: Optional D2 (Value (String))
} deriving (Generic, Eq, Show)

instance Encode DataSourceResponseBurst
instance Decode DataSourceResponseBurst

