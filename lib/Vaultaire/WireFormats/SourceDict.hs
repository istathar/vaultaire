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

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Vaultaire.WireFormats.SourceDict
(
    SourceDict,
    unionSource,
    diffSource,
    makeSourceDict,
    module Vaultaire.WireFormats.Class
) where

import Control.Applicative (optional, many, (<$>), (<*), (<*>))
import Control.Exception (SomeException (..))
import Data.Attoparsec.Text (parseOnly, (<*.))
import qualified Data.Attoparsec.Text as PT
import Data.ByteString.Builder (byteString, toLazyByteString)
import Data.ByteString.Lazy (toStrict)
import Data.HashMap.Strict (HashMap, difference, foldlWithKey', fromList,
                            union)
import Data.Monoid (Monoid, (<>))
import Data.Text (Text, find)
import Data.Text.Encoding (decodeUtf8', encodeUtf8)
import Data.Maybe(isNothing)
import Vaultaire.WireFormats.Class

newtype SourceDict = SourceDict { unSourceDict :: HashMap Text Text }
  deriving (Show, Eq, Monoid)

makeSourceDict :: HashMap Text Text -> Either String SourceDict
makeSourceDict hm = if foldlWithKey' allGoodKV True hm
                    then Right $ SourceDict hm
                    else Left "Bad character in source dict,\
                              \ no ',', or ':' allowed."
  where allGoodKV acc k v = acc && (allGoodChars k && allGoodChars v)
        allGoodChars = isNothing . find (\c -> c == ':' || c == ',')

instance WireFormat SourceDict where
    fromWire bs = either (Left . SomeException) parse (decodeUtf8' bs)
      where
        parse t = either (Left . SomeException . userError)
                         (Right . SourceDict . fromList)
                         (parseOnly tagParser t)

        tagParser = many $ (,) <$> k <*> v
          where
            k = PT.takeWhile (/= ':') <*. ":"
            v = PT.takeWhile (/= ',') <* optional ","

    toWire = toStrict . toLazyByteString . foldlWithKey' f "" . unSourceDict
      where f acc k v = acc <> text k <> ":" <> text v <> ","
            text = byteString . encodeUtf8

unionSource :: SourceDict -> SourceDict -> SourceDict
unionSource (SourceDict a) (SourceDict b) = SourceDict $ union a b

diffSource :: SourceDict -> SourceDict -> SourceDict
diffSource (SourceDict a) (SourceDict b) = SourceDict $ difference a b
