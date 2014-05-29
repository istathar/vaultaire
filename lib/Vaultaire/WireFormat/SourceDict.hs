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

module Vaultaire.WireFormat.SourceDict
(
    SourceDict,
    unionSource,
    diffSource,
    makeSourceDict,
) where

import Blaze.ByteString.Builder (fromByteString, toByteString)
import Blaze.ByteString.Builder.Char8 (fromChar)
import Control.Applicative (many, optional, (<$>), (<*), (<*>))
import Control.Exception (SomeException (..))
import Data.Attoparsec.Text (parseOnly, (<*.))
import qualified Data.Attoparsec.Text as PT
import Data.HashMap.Strict (HashMap, difference, foldlWithKey', fromList,
                            union)
import Data.Maybe (isNothing)
import Data.Monoid (Monoid, mempty, (<>))
import Data.Text (Text, find)
import Data.Text.Encoding (decodeUtf8', encodeUtf8)
import Vaultaire.WireFormat.Class

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

    toWire = toByteString . foldlWithKey' f mempty . unSourceDict
      where
        f acc k v = acc <> text k <> fromChar ':' <> text v <> fromChar ','
        text = fromByteString . encodeUtf8

unionSource :: SourceDict -> SourceDict -> SourceDict
unionSource (SourceDict a) (SourceDict b) = SourceDict $ union a b

diffSource :: SourceDict -> SourceDict -> SourceDict
diffSource (SourceDict a) (SourceDict b) = SourceDict $ difference a b
