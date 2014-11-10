{-# LANGUAGE MagicHash #-}
module Vaultaire.Profiler
     ( Period
     , startProfiler
     , profileCount
     , profileTime )
where

import           Control.Applicative
import           Control.Concurrent
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import qualified Data.Map.Strict as M
import           Pipes
import           Pipes.Concurrent
import           Pipes.Lift
import           Pipes.Parse (foldAll)
import qualified System.ZMQ4 as Z

import           Vaultaire.Types
import           Vaultaire.Daemon

-- TODO
-- the profiler also needs to check shutdown periodically
-- if the profiler crashes, the worker shouldn't be pulled down
-- but if the worker dies, the profiler should die too

type PublishSock = Z.Socket Z.Pub

startProfiler :: DaemonArgs -> IO ()
startProfiler args =
    Z.withContext $ \ctx ->
      Z.withSocket ctx Z.Pub $ \sock -> do
        Z.connect sock $ broker args
        runDaemon args (profile sock)

profileCount :: TeleMsgType -> Origin -> Daemon ()
profileCount t g = do
    (_, prof) <- ask
    profCount prof t g

profileTime :: TeleMsgType -> Origin -> Daemon r -> Daemon r
profileTime  t g act = do
    (_, prof) <- ask
    profTime prof t g act

profile :: PublishSock -> Daemon ()
profile sock = do
    (_, c) <- ask
    -- Read at most N reports from the profiling channel (N = size of the channel)
    -- since new reports would still be coming in after we have commenced this operation.
    msgs <- aggregate $ fromInputUntil (bound c) (inchan c)
    _    <- mapM (mkResp (aname c) >=> publish sock) msgs
    -- Sleep for <period>
    liftIO $ threadDelay (sleep c)
    where fromInputUntil n chan = evalStateP 0 $ do
            x      <- lift $ get
            when (x <= n) $ fromInput chan
          mkResp n msg = do
            t      <- liftIO $ getCurrentTimeNanoseconds
            return $ TeleResp t n msg

publish :: PublishSock -> TeleResp -> Daemon ()
publish sock resp =
    liftIO $ Z.send sock [] $ toWire resp

-- | Aggregate telemetric reports, guaranteed to process only N reports
--   see @report@.
--
aggregate :: Monad m => Producer TeleMsg m () -> m [TeleMsg]
aggregate = evalStateT $ foldAll
  (\acc x -> M.insertWith (go $ _type x) (_origin x, _type x) (1, _payload x) acc)
  (M.empty)
  (map (uncurry extract) . M.toList)
  where go WriterSimplePoints       = count
        go WriterExtendedPoints     = count
        go WriterRequest            = count
        go WriterRequestLatency     = keep
        go WriterCephLatency        = keep
        go ReaderSimplePoints       = count
        go ReaderExtendedPoints     = count
        go ReaderRequest            = count
        go ReaderRequestLatency     = keep
        go ReaderCephLatency        = keep
        go ContentsEnumerate        = count
        go ContentsUpdate           = count
        go ContentsEnumerateLatency = keep
        go ContentsUpdateLatency    = keep
        go ContentsEnumerateCeph    = keep
        go ContentsUpdateCeph       = keep
        extract k@(_, WriterSimplePoints      ) = msg k <$> (fst                )
        extract k@(_, WriterExtendedPoints    ) = msg k <$> (fst                )
        extract k@(_, WriterRequest           ) = msg k <$> (fst                )
        extract k@(_, WriterRequestLatency    ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, WriterCephLatency       ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ReaderSimplePoints      ) = msg k <$> (fst                )
        extract k@(_, ReaderExtendedPoints    ) = msg k <$> (fst                )
        extract k@(_, ReaderRequest           ) = msg k <$> (fst                )
        extract k@(_, ReaderRequestLatency    ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ReaderCephLatency       ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ContentsEnumerate       ) = msg k <$> (fst                )
        extract k@(_, ContentsUpdate          ) = msg k <$> (fst                )
        extract k@(_, ContentsEnumerateLatency) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ContentsUpdateLatency   ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ContentsEnumerateCeph   ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ContentsUpdateCeph      ) = msg k <$> (div <$> snd <*> fst)
        count (c1, _)  (c2, _)  = (c1 + c2, 0)
        keep  (c1, v1) (c2, v2) = (c1 + c2, v1 + v2)
        msg (x,y) z = TeleMsg x y z
