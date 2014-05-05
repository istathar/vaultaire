{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
module Main where

import Control.Monad
import Control.Monad.Error
import Options.Applicative
import qualified Data.ByteString.Char8 as BS
import qualified System.ZMQ4.Monadic as Zero
import qualified Control.Concurrent.Async as Async

import Marquise.Config

data CmdOptions = CmdOptions {
    optConfigFile :: !String,
    optDebug :: !Bool
}

printDebug :: MonadIO m => Bool -> String -> m ()
printDebug d msg = liftIO $
    when d $ putStrLn msg

opts :: Parser CmdOptions
opts = CmdOptions
    <$> strOption
        (long "config-file"
         <> short 'c'
         <> value "/etc/vaultaire/marquised.conf"
         <> metavar "CONFIGFILE"
         <> help "Path to configuration file." )
    <*> switch
        (long "debug" <>
         short 'd' <>
         help "Enable debugging output.")

marquiseOptionParser :: ParserInfo CmdOptions
marquiseOptionParser = info (helper <*> opts)
    (fullDesc <>
        progDesc "Marquise daemon" <>
        header "marquised - daemon for writing datapoints to Vaultaire")

marquiseD :: CmdOptions -> IO ()
marquiseD (CmdOptions cfgFile debug) = do
    cfg <- liftIO $ readConfigFile cfgFile
    case cfg of
        Nothing -> do
            putStrLn "Failed to parse config."
        Just ConfigOptions{..} -> 
            Zero.runZMQ $ do
                let writer = "tcp://" ++ writerAddr ++ ":5560"
                printDebug debug ("Listening on " ++ listenAddr)
                printDebug debug ("Writing to broker at " ++ writer)
                router <- Zero.socket Zero.Router
                Zero.setReceiveHighWM (Zero.restrict 0) router
                Zero.bind router listenAddr
                forever $ do 
                    msg <- Zero.receive router
                    liftIO $ printDebug debug $ "Got a message: " ++ (BS.unpack msg)
  where
    linkThread f = Zero.async f >>= liftIO . Async.link

main :: IO ()
main = do
    options <- execParser marquiseOptionParser
    marquiseD options
