{-# LANGUAGE MagicHash #-}
module Vaultaire.Profiler
     ( Period
     , startProfiler
     , noProfiler
     , hasProfiler
     , profileCount
     , profileTime )
where

import           Control.Applicative
import           Control.Concurrent
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import qualified Data.Map.Strict as M
import           Data.UnixTime
import           Data.Monoid
import           Pipes
import           Pipes.Concurrent
import           Pipes.Lift
import           Pipes.Parse (foldAll)
import           System.Log.Logger
import qualified System.ZMQ4 as Z

import           Vaultaire.Types
import           Vaultaire.Daemon


type PublishSock = Z.Socket Z.Pub

startProfiler :: DaemonArgs -> Period -> IO ()
startProfiler args period =
    Z.withContext $ \ctx ->
      Z.withSocket ctx Z.Pub $ \sock -> do
        Z.connect sock $ broker args
        runDaemon args (profile period sock)

noProfiler :: Profiler
noProfiler =  Profiler
    { aname     = mempty
    , bound     = 0
    , outchan   = Output { send = const $ return False}
    , inchan    = Input  { recv = return Nothing }
    , seal      = return ()
    , profCount = const $ const $ return ()
    , profTime  = const $ const id }

hasProfiler :: String -> Period -> IO Profiler
hasProfiler name period =  do
    n <- maybe (do errorM  "Daemon.setupProfiler"
                          ("The daemon name given is invalid: " ++ name ++
                           ". An empty name has been given to the daemon.")
                   return mempty)
               (return)
               (agentID name)
    -- We use the @Newest@ buffer for the internal report queue
    -- so that old reports will be removed if the buffer is full.
    -- This means the internal will lose precision but not have
    -- an impact on performance if there is too much activity.
    (output, input, sealchan) <- spawn' $ Newest 1024
    return $ Profiler
           { aname     = n
           , bound     = 1024
           , outchan   = output
           , inchan    = input
           , seal      = liftIO $ atomically sealchan
           , profCount = g output
           , profTime  = h output }
    where g outchan timestamp origin = liftIO $ do
            _ <- atomically (send outchan $ TeleMsg origin timestamp 1)
            return ()
          h outchan timestamp origin act = do
            t1 <- liftIO $ getUnixTime
            r  <- act
            t2 <- liftIO $ getUnixTime
            _  <- liftIO $ atomically $ send outchan
               $  TeleMsg origin timestamp $ fromIntegral
               $  udtMicroSeconds
               $  diffUnixTime t1 t2
            return r

profileCount :: TeleMsgType -> Origin -> Daemon ()
profileCount t g = do
    (_, prof) <- ask
    profCount prof t g

profileTime :: TeleMsgType -> Origin -> Daemon r -> Daemon r
profileTime  t g act = do
    (_, prof) <- ask
    profTime prof t g act

profile :: Period -> PublishSock -> Daemon ()
profile period sock = do
    (_, c) <- ask
    -- Read at most N reports from the profiling channel (N = size of the channel)
    -- since new reports would still be coming in after we have commenced this operation.
    msgs <- aggregate $ fromInputUntil (bound c) (inchan c)
    _    <- mapM (mkResp (aname c) >=> publish sock) msgs
    -- Sleep for <period>
    liftIO $ threadDelay period
    where fromInputUntil n chan = evalStateP 0 $ do
            x      <- lift $ get
            when (x <= n) $ fromInput chan
          mkResp n msg = do
            t      <- liftIO $ getCurrentTimeNanoseconds
            return $ TeleResp t n msg

publish :: PublishSock -> TeleResp -> Daemon ()
publish sock resp = do
    (_, c) <- ask
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
