--
-- Data vault for metrics
--
-- Copyright © 2013-2014 Anchor Systems, Pty Ltd and Others
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the 3-clause BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module Vaultaire.Contents
     ( -- * Contents Daemon
       startContents
     ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.Bits
import Data.Maybe (isJust)
import Data.Monoid (mempty)
import Data.Word (Word64)
import Pipes
import System.Log.Logger
import System.Random
import Vaultaire.Daemon
import qualified Vaultaire.InternalStore as InternalStore
import Vaultaire.Types

-- | Start a contents daemon, never returns.
--
startContents :: DaemonArgs -> IO ()
startContents = flip handleMessages handleRequest

-- | Perform the action requested in the 'Message' and send the
--   appropriate response to the client.
handleRequest :: Message -> Daemon ()
handleRequest (Message reply origin payload) =
    case fromWire payload of
        Left err -> liftIO $ errorM "Contents.handleRequest" $
                                    "bad request: " ++ show err
        Right op -> case op of
            ContentsListRequest   -> profileCount ContentsEnumerate origin
                                  >> performListRequest reply origin

            GenerateNewAddress    -> performRegisterRequest reply origin

            UpdateSourceTag a s   -> profileCount ContentsUpdate origin
                                  >> performUpdateRequest reply origin a s

            RemoveSourceTag a s   -> performRemoveRequest reply origin a s

{-
    For the given address, read all the contents entries matching it. The
    latest entry is deemed most correct. Return that blob. No attempt is made
    to decode it; after all, the only way it could get in there is via the
    update or remove opcodes.

    The use of a Pipe here allows us to stream the responses back to the
    requesting client. Note that reply with Response can be used multiple
    times, so each reply here represents one Address,SourceDict pair.
-}
performListRequest :: ReplyF -> Origin ->  Daemon ()
performListRequest reply o
  = profileTime ContentsEnumerateLatency o $ do

    liftIO $ infoM "Contents.performListRequest"
                (show o ++ " ContentsListRequest")

    runEffect $ for (InternalStore.enumerateOrigin o)
                    (lift . reply . uncurry ContentsListBypass)
    reply EndOfContentsList

-- | A request has been made to allocate a new unique address; do this
--   and return it to the client.
performRegisterRequest :: ReplyF -> Origin -> Daemon ()
performRegisterRequest reply o = do
    liftIO $ infoM "Contents.performRegisterRequest"
                (show o ++ " RegisterListRequest")

    allocateNewAddressInVault o >>= reply . RandomAddress

-- | Generate a random address, make sure it's unused, and write an
--   empty source dict for it so it is no longer unused.
allocateNewAddressInVault :: Origin -> Daemon Address
allocateNewAddressInVault o = do
    a <- Address . (`clearBit` 0) <$> liftIO rollDice

    withLockExclusive "02_addresses_lock" $ do
        exists <- isAddressInVault a
        if exists
            then allocateNewAddressInVault o
            else do
                writeSourceTagsForAddress o a mempty
                return a
  where
    rollDice = getStdRandom (randomR (0, maxBound :: Word64))
    isAddressInVault a = isJust <$> InternalStore.readFrom o a

-- | Update the sourcedict associated with the provided address. New
--   tags will be added; new values for existing names will be updated
--   (in the case of sourcedict objects only, last write wins);
--   no tags will be removed.
performUpdateRequest
    :: ReplyF
    -> Origin
    -> Address
    -> SourceDict
    -> Daemon ()
performUpdateRequest reply o a input
  = profileTime ContentsUpdateLatency o $ do

    liftIO $ infoM "Contents.performUpdateRequest"
                (show o ++ " UpdateRequest " ++ show a)

    (result, readTime) <- elapsed $ retreiveSourceTagsForAddress o a

    case result of
        Nothing -> do
            (_, writeTime) <- elapsed $ writeSourceTagsForAddress o a input
            -- NOTE: measurement of Ceph latency for Contents is not as accurate as
            --       for Reader/Writer, since these times include cycles spent in
            --       Reader/Writer as well as Rados.
            profileReport ContentsUpdateCeph o (readTime + writeTime)

        Just current -> do
            -- items in first SourceDict (the passed in update from user) win
            let update = unionSource input current
            unless (current == update) (writeSourceTagsForAddress o a update)
    reply UpdateSuccess

-- | Remove the tags specified in the provided sourcedict from the
--   provided address. Tags not specified in the provided sourcedict
--   will remain.
performRemoveRequest
    :: ReplyF
    -> Origin
    -> Address
    -> SourceDict
    -> Daemon ()
performRemoveRequest reply o a input = do
    liftIO $ infoM "Contents.performRemoveRequest"
                (show o ++ " RemoveRequest " ++ show a)

    result <- retreiveSourceTagsForAddress o a
    -- elements of first SourceDict not appearing in second remain
    case result of
        Nothing -> return ()
        Just current -> do
            let update = diffSource current input
            unless (current == update) (writeSourceTagsForAddress o a update)

    liftIO $ infoM "Contents.performRemoveRequest"
                (show o ++ " Complete")
    reply RemoveSuccess

-- | Read the sourcedict associated with an address.
retreiveSourceTagsForAddress :: Origin -> Address -> Daemon (Maybe SourceDict)
retreiveSourceTagsForAddress o a = do
    result <- InternalStore.readFrom o a
    return $ case result of
        Just b'     -> either throw Just (fromWire b')
        Nothing     -> Nothing

-- | Pack the tags in the provided sourcedict and write them to Ceph.
--   This will overwrite any previously-associated sourcedict for that
--   address.
writeSourceTagsForAddress :: Origin -> Address -> SourceDict -> Daemon ()
writeSourceTagsForAddress o a s = do
    liftIO $ infoM "Contents.writeSourceTagsForAddress"
                (show o ++ " Writing SourceDict")

    InternalStore.writeTo o a (toWire s)
