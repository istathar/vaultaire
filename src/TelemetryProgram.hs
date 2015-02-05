{-# LANGUAGE OverloadedStrings #-}

import Control.Exception
import Network.URI
import Options.Applicative
import System.ZMQ4 hiding (shutdown)

import Vaultaire.Types

helpfulParser :: ParserInfo String
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser String
optionsParser = parseBroker
  where
    parseBroker = strOption $
           long "broker"
        <> short 'b'
        <> metavar "BROKER"
        <> value "localhost"
        <> showDefault
        <> help "Vault broker host name or IP address"

main :: IO ()
main = do
    broker <- execParser helpfulParser
    maybe (putStrLn "Invalid broker URI")
          runTelemetrySub
          (parseURI $ "tcp://" ++ broker ++ ":6660")

runTelemetrySub :: URI -> IO ()
runTelemetrySub broker =
    -- connect to the broker for telemtrics
    withContext $ \ctx ->
      withSocket ctx Sub $ \sock -> do
        connect sock $ show broker
        subscribe sock ""
        go sock
    where go sock = do
            x <- receive sock
            case (fromWire x :: Either SomeException TeleResp) of
              Right y -> print y
              _       -> putStrLn "Unrecognised telemetric response."
            go sock
