{-# LANGUAGE OverloadedStrings #-}
import           Control.Exception
import           Network.URI
import           System.ZMQ4 hiding (shutdown)
import           System.ZMQ4.Monadic (runZMQ)
import           System.Environment

import           Vaultaire.Broker
import           Vaultaire.Types
import           Vaultaire.Util

main :: IO ()
main = do
    args <- getArgs
    case args of broker:_ -> maybe (putStrLn "Invalid broker URI")
                                   (runTelemetrySub)
                                   (parseURI $ "tcp://" ++ broker ++ ":6660")
                 _        -> putStrLn "Specify a broker URI."

runTelemetrySub :: URI -> IO ()
runTelemetrySub broker = do
    -- setup a broker for telemetry
    linkThread $ do
        runZMQ $ startProxy (XPub,"tcp://*:6660")
                            (XSub,"tcp://*:6661")
                            "tcp://*:6000"

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
