{-# LANGUAGE OverloadedStrings          #-}

-- | Basic proxying broker
module Main where
import System.ZMQ4.Monadic
import Control.Concurrent.Async(link)
import Control.Concurrent(threadDelay)

main :: IO ()
main = runZMQ $ do
    startProxy (Router,"tcp://*:5560") (Dealer,"tcp://*:5561") "tcp://*:5000"
    wait

wait :: ZMQ z ()
wait = liftIO (threadDelay maxBound) >> wait

startProxy :: (SocketType front_t, SocketType back_t)
           => (front_t, String) -- | Frontend, clients
           -> (back_t, String)  -- | Backend, workers
           -> String            -- | Capture address, for debug
           -> ZMQ z ()
startProxy (front_type, front_addr) (back_type, back_addr) cap_addr =
    linkThread $ do
        front_s <- socket front_type
        back_s <- socket back_type
        cap_s <- socket Pub

        bind front_s front_addr
        bind back_s back_addr
        bind cap_s cap_addr

        proxy front_s back_s (Just cap_s)

linkThread :: ZMQ z a -> ZMQ z ()
linkThread a = async a >>= liftIO . link
