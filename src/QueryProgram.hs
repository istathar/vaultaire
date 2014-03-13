{-# LANGUAGE DeriveDataTypeable       #-}
{-# LANGUAGE DeriveGeneric            #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE NamedFieldPuns           #-}
{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE PackageImports           #-}

module QueryProgram where

import Control.Applicative
import qualified Data.Attoparsec.Char8 as Atto
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.ProtocolBuffers
import Data.Serialize (runGet, runPut)
import Data.Word (Word64)
import Options.Applicative
import System.ZMQ4.Monadic hiding (source)
import Vaultaire.Serialize.Common (SourceTag (..))
import Vaultaire.Serialize.WireFormat (DataBurst (..), RequestMulti (..),
                                       RequestSource (..))

type Broker = String
type Origin = String

data Options = Options {
    broker :: Broker,
    source :: String,
    from   :: Int,
    to     :: Int,
    origin :: Origin
}

program :: Options -> IO ()
program (Options{broker, source, origin, from, to}) = do
    case Atto.parseOnly tagParser (B.pack source) of
        Left e ->
            putStrLn $ "failed to parse source: " ++ e
        Right tags -> do
            putStrLn $ "Requesting source " ++ show tags ++
                       " from origin " ++ show origin
            let request = requestFromTags tags from to
            makeRequest broker origin request

requestFromTags :: [SourceTag] -> Int -> Int -> RequestMulti
requestFromTags tags from to =
    let requests = [RequestSource (putField tags) from' to']
        from'    = putField $ fromEpoch from
        to'      = putField $ fromEpoch to
    in RequestMulti $ putField requests

makeRequest :: Broker -> Origin -> RequestMulti -> IO ()
makeRequest broker origin request = do
    runZMQ $ do
        s <- socket Req
        connect s ("tcp://" ++ broker ++ ":5570")
        send s [SendMore] $ B.pack origin
        send s [] $ encodeRequest request
        printLoop s
  where
    encodeRequest = runPut . encodeMessage

    decodeBurst :: ByteString -> Either String DataBurst
    decodeBurst = runGet decodeMessage

    printLoop s = do
        msg <- receive s
        if B.null msg
            then liftIO $ putStrLn "EOF"
            else do
                either error (liftIO . print) (decodeBurst msg)
                printLoop s


toplevel :: Parser Options
toplevel = Options
    <$> argument str
            (metavar "BROKER" <>
             help "Host name or IP address of broker")
    <*> argument str
            (metavar "SOURCE" <>
             help "Source you wish to request, written as k~v,k2~v2" )
    <*> argument auto
            (metavar "FROM" <>
             help "Unix epoch time for request to start from" )
    <*> argument auto
            (metavar "TO" <>
             help "Unix epoch time for end of request" )
    <*> argument str
            (metavar "ORIGIN" <>
            help "Origin in which source resides")

tagParser :: Atto.Parser [SourceTag]
tagParser = some $ SourceTag <$> k <*> v
  where
    k = putField <$> Atto.takeWhile1 (/= '~') <* "~"
    v = putField <$> Atto.takeWhile1 (/= ',') <* optional ","

fromEpoch :: Int -> Fixed Word64
fromEpoch = fromIntegral . (* 1000000000)

commandLineParser :: ParserInfo Options
commandLineParser = info (helper <*> toplevel)
            (fullDesc <>
                progDesc "Query utility to make requests for sources" <>
                header "Vaultaire query utility")
