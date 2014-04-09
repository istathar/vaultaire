module Vaultaire.Broker
(
    startProxy,
) where

import System.ZMQ4.Monadic

-- | Start a ZMQ proxy, capture is always a Pub socket.
--
-- This should never return lest catastrophic failure.
startProxy :: (SocketType front_t, SocketType back_t)
           => (front_t, String) -- ^ Frontend, clients
           -> (back_t, String)  -- ^ Backend, workers
           -> String            -- ^ Capture address, for debug
           -> ZMQ z ()
startProxy (front_type, front_addr) (back_type, back_addr) cap_addr = do
    front_s <- socket front_type
    back_s <- socket back_type
    cap_s <- socket Pub

    bind front_s front_addr
    bind back_s back_addr
    bind cap_s cap_addr

    proxy front_s back_s (Just cap_s)
