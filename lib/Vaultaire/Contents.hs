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

module Vaultaire.Contents
(
    SourceTag(..),
    DataRequest(..),
    DataRequestMulti(..),
    ContentsResponse(..),
    ContentsResponseBurst(..)
) where

import Data.ProtocolBuffers hiding (field)
import Data.Text (Text)
import qualified Data.Text as T
import Data.TypeLevel (D1, D2, D3)
import Data.Word (Word64)
import GHC.Generics (Generic)


data SourceTag = SourceTag {
    field :: Required D1 (Value Text),
    value :: Required D2 (Value Text)
} deriving (Generic, Eq)

instance Encode SourceTag
instance Decode SourceTag

instance Show SourceTag where
    show x =
        k ++ ":" ++ v
      where
        k = T.unpack $ getField $ field x
        v = T.unpack $ getField $ value x

{-
data ValueType
    = EMPTY
    | NUMBER
    | REAL
    | TEXT
    | BINARY
  deriving (Enum, Generic, Show, Eq)

instance Encode ValueType
instance Decode ValueType


data DataFrame = DataFrame {
    source           :: Repeated D1 (Message SourceTag),
    origin           :: Optional D8 (Value ByteString)
} deriving (Generic, Eq)

instance Encode DataFrame
instance Decode DataFrame
-}

--
-- Query request
--

data DataRequest = Request {
    requestSourceField :: Repeated D1 (Message SourceTag),
    requestAlphaField  :: Required D2 (Value (Fixed Word64)),
    requestOmegaField  :: Required D3 (Value (Fixed Word64))
} deriving (Generic, Eq, Show)

instance Encode DataRequest
instance Decode DataRequest

data DataRequestMulti = DataRequestMulti {
    multiRequestsField :: Repeated D1 (Message DataRequest)
} deriving (Generic, Eq, Show)

instance Encode DataRequestMulti
instance Decode DataRequestMulti

--
-- Contents response to chevalier
--

data ContentsResponse = ContentsResponse {
    responseSourceField :: Repeated D1 (Message SourceTag)
} deriving (Generic, Eq, Show)

instance Encode ContentsResponse
instance Decode ContentsResponse

data ContentsResponseBurst = ContentsResponseBurst {
    responseSourcesField :: Repeated D1 (Message ContentsResponse),
    responseErrorField   :: Optional D2 (Value Text)
} deriving (Generic, Eq, Show)

instance Encode ContentsResponseBurst
instance Decode ContentsResponseBurst




