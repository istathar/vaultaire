module Main where
import Vaultaire.Broker
import Vaultaire.Util
import System.ZMQ4.Monadic

main :: IO ()
main = runZMQ $ do
    async $ startProxy (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
    waitForever
