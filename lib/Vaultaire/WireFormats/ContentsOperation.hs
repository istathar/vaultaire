{-# LANGUAGE OverloadedStrings #-}
module Vaultaire.WireFormats.ContentsOperation
(
    ContentsOperation(..),
    SourceDict,
    handleSourceArgument,
    module Vaultaire.WireFormats.Class
) where

import Data.HashMap.Strict (HashMap, fromList)
import Data.Text (Text)
import Vaultaire.CoreTypes (Address(..))
import Vaultaire.WireFormats.Class
import qualified Data.ByteString.Char8 as BS
import Data.Packer(tryUnpacking, getWord64LE, getBytes, Unpacking)
import Control.Applicative((<$>))
import Data.ByteString(ByteString)
import qualified Data.Text.Encoding as T

type SourceDict = HashMap Text Text

data ContentsOperation = ContentsListRequest
                       | GenerateNewAddress
                       | UpdateSourceTag Address SourceDict
                       | RemoveSourceTag Address SourceDict
  deriving (Show, Eq)

instance WireFormat ContentsOperation where
    fromWire bs = flip tryUnpacking bs $ do
        word <- getWord64LE
        case word of
            0x0 ->
                return ContentsListRequest
            0x1 ->
                return GenerateNewAddress
            0x2 -> do
                a <- Address <$> getWord64LE
                s <- parseSourceDict
                return (UpdateSourceTag a s)
            0x3 -> do
                a <- Address <$> getWord64LE
                s <- parseSourceDict
                return (RemoveSourceTag a s)
            _   -> fail "Illegal op code"

    toWire op =
        case op of
            ContentsListRequest   -> "\x00\x00\x00\x00\x00\x00\x00\x00"
            GenerateNewAddress    -> "\x01\x00\x00\x00\x00\x00\x00\x00"
            UpdateSourceTag _ _   -> undefined -- 0x2 
            RemoveSourceTag _ _   -> undefined -- 0x3



parseSourceDict :: Unpacking SourceDict
parseSourceDict = do
    len  <- fromIntegral <$> getWord64LE
    handleSourceArgument <$> getBytes len

{-
    We should replace this with a proper parser in order to get error
    reporting.

    FIXME: Needs to handle UTF8 decode errors
-}
handleSourceArgument :: ByteString -> SourceDict
handleSourceArgument b' =
  let
    items' = BS.split ',' b'
    pairs' = map (BS.split ':') items'
    pairs  = map toTag pairs'
  in
    fromList pairs
  where
    toTag :: [ByteString] -> (Text, Text)
    toTag [k',v'] = (T.decodeUtf8 k', T.decodeUtf8 v')
    toTag _ = error "invalid source argument"


