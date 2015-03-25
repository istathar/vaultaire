--
-- Common Haskell build tools
--
-- Copyright © 2013-2014 Operational Dynamics Consulting, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

import Data.Char (toUpper)
import Distribution.PackageDescription (PackageDescription (..))
import Distribution.Simple
import Distribution.Simple.LocalBuildInfo (LocalBuildInfo)
import Distribution.Simple.Setup (ConfigFlags)
import Distribution.System (OS (..), buildOS)
import Distribution.Text (display)
import System.IO (Handle, IOMode (..), hPutStrLn, withFile)

main :: IO ()
main = defaultMainWithHooks $ simpleUserHooks {
       postConf = configure
    }

{-
    Simple detection of which operating system we're building on;
    there's no need to link the Cabal logic into our library, so
    we output to a .hs file in src/
-}

configure :: Args -> ConfigFlags -> PackageDescription -> LocalBuildInfo -> IO ()
configure _ _ p _  = do
    withFile "src/Package.hs" WriteMode (\h -> do
        outputModuleHeader h
        discoverOperatingSystem h
        discoverProgramVersion h p)

    return ()

outputModuleHeader :: Handle -> IO ()
outputModuleHeader h = do
    hPutStrLn h "module Package where"

discoverOperatingSystem :: Handle -> IO ()
discoverOperatingSystem h = do
    hPutStrLn h "build :: String"
    hPutStrLn h ("build = \"" ++ s ++ "\"")
  where
    o = buildOS

    s = case o of
            Linux   -> "Linux"
            OSX     -> "Mac OS X"
            Windows -> "Windows"
            _       -> up o

    up x = map toUpper (show x)

discoverProgramVersion :: Handle -> PackageDescription -> IO ()
discoverProgramVersion h p = do
    hPutStrLn h "package :: String"
    hPutStrLn h ("package = \"" ++ n ++ "\"")
    hPutStrLn h "version :: String"
    hPutStrLn h ("version = \"" ++ s ++ "\"")
  where
    i = package p
    (PackageName n) = pkgName i
    v = pkgVersion i
    s = display v

