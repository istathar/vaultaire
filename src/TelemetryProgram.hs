{-# LANGUAGE OverloadedStrings #-}
import           Control.Exception
import           Network.URI
import           System.ZMQ4 hiding (shutdown)
import           System.Environment

import           Vaultaire.Types

main :: IO ()
main = do
    args <- getArgs
    case args of broker:_ -> maybe (putStrLn "Invalid broker URI")
                                   (runTelemetrySub)
                                   (parseURI $ "tcp://" ++ broker ++ ":6660")
                 _        -> putStrLn "Specify a broker URI."

runTelemetrySub :: URI -> IO ()
runTelemetrySub broker = do
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
