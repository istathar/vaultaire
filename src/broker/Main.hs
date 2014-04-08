module Main where
import Vaultaire.Broker
import System.ZMQ4.Monadic

main :: IO ()
main = runZMQ $ do
    linkThread $ startProxy (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
    wait
