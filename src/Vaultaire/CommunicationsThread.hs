module CommunicationsThread where

import Control.Concurrent.STM.TChan
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.ByteString.Char8 as S
import Data.List.NonEmpty (fromList)
import GHC.Conc
import Network.BSD (getHostName)
import Options.Applicative
import System.Environment (getProgName)
import System.Posix.Process (getProcessID)
import System.ZMQ4.Monadic (runZMQ)
import qualified System.ZMQ4.Monadic as Zero
import Text.Printf


type Telemetry = (String,String,String)

telemetry_sender :: String -> Bool -> TChan Telemetry  -> IO ()
telemetry_sender broker d telemetry =
    runZMQ $ do
        (identifier, hostname) <- liftIO getIdentifierAndHostname

        tele <- Zero.socket Zero.Pub
        Zero.connect tele ("tcp://" ++ broker ++ ":5581")

        forever $ do
            (k,v,u) <- liftIO . atomically $ readTChan telemetry
            when d $ liftIO $ putStrLn $ printf "%-10s %-9s %s" (k ++ ":") v u
            let reply = [S.pack k, S.pack v, S.pack u, identifier, hostname]
            Zero.sendMulti tele (fromList reply)
  where
    getIdentifierAndHostname = do
        hostname <- S.pack <$> getHostName
        pid  <- getProcessID
        name <- getProgName
        let identifier = S.pack (name ++ "/" ++ show pid)
        return (identifier, hostname)


