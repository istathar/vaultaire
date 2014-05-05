module Marquise.Config
(
    ConfigOptions(..),
    readConfigFile
) where

import Control.Applicative
import Text.Trifecta

data ConfigOptions = ConfigOptions {
    listenAddr :: !String,
    writerAddr :: !String,
    contentsAddr :: !String,
    deferralDir :: !String
} deriving (Eq, Ord, Show)

(<$!>) :: (a -> b) -> Parser a -> Parser b
f <$!> ma = do
    a <- ma
    return $! f a

eol :: Parser ()
eol = skipSome (oneOf "\n\r") <?> "eol"

skipEol :: Parser ()
skipEol = skipSome (noneOf "\n\r")

comment :: Parser ()
comment = 
    char '#' *> skipEol <?> "comment" 

value :: Parser String
value = manyTill anyChar (try eol <|> try comment <|> eof)


variable :: String -> Parser String 
variable name = (string name <?> name)
                <* skipMany (char ' ')
                <* char '='
                <* value


opts :: Parser ConfigOptions
opts = ConfigOptions <$!> (variable "listen_address")
       <*> (variable "writer_address")
       <*> (variable "contents_address")
       <*> (variable "deferral_dir") 

readConfigFile :: String -> IO (Maybe ConfigOptions)
readConfigFile fname = parseFromFile opts fname
