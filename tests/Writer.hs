{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.ByteString.Lazy.Builder
import Data.Monoid
import Test.Hspec hiding (pending)
import Control.Monad.State.Strict
import qualified Data.HashMap.Strict as HashMap
import Control.Applicative
import Vaultaire.Writer
import Vaultaire.DayMap
import Data.Time

main :: IO ()
main = do
    now <- getCurrentTime
    hspec (suite now)

suite :: UTCTime -> Spec
suite now = do
    describe "appendSimple" $ do
        it "generates a single builder from one append" $ do
            let st = execState (appendSimple 0 1 "HAI") (startState now)
            let builder = HashMap.lookup 0 (normal st) >>= HashMap.lookup 1
            case builder of Nothing -> error "lookup"
                            Just b  -> toLazyByteString b `shouldBe` "HAI"

        it "generates a single builder from one append" $ do
            let st = execState (appendSimple 0 1 "A" >> appendSimple 0 1 "B")
                               (startState now)
            let builder = HashMap.lookup 0 (normal st) >>= HashMap.lookup 1
            case builder of Nothing -> error "lookup"
                            Just b  -> toLazyByteString b `shouldBe` "AB"

    describe "appendExtended" $ do
        it "creates appropriate builders and extended map" $ do
            let st = execState (appendExtended 0 1 0x42 0x90 2 "BC")
                               (startState now)

            let ext_bytes = "\x02\x00\x00\x00\x00\x00\x00\x00\&BC"
            let ext = HashMap.lookup 0 (extended st) >>= HashMap.lookup 1
            case ext of Nothing -> error "lookup extended"
                        Just b  -> toLazyByteString b `shouldBe` ext_bytes

            let pend = HashMap.lookup 0 (pending st) >>= HashMap.lookup 1
            let pend_bytes = "\x42\x00\x00\x00\x00\x00\x00\x00\
                             \\x90\x00\x00\x00\x00\x00\x00\x00\
                             \\10\x00\x00\x00\x00\x00\x00\x00"
            case pend of
                Nothing -> error "lookup pending"
                Just fs -> let b = mconcat $ map ($10) (snd fs)
                           in toLazyByteString b `shouldBe` pend_bytes
startDayMaps :: (DayMap, DayMap)
startDayMaps =
    let simple   = loadDayMap "\x00\x00\x00\x00\x00\x00\x00\x00\
                              \\x42\x00\x00\x00\x00\x00\x00\x00"
        ext      = loadDayMap "\x00\x00\x00\x00\x00\x00\x00\x00\
                              \\x42\x00\x00\x00\x00\x00\x00\x00"
    in either error id $ (,) <$> simple <*> ext

startState :: UTCTime -> BatchState
startState = BatchState mempty mempty mempty mempty startDayMaps
