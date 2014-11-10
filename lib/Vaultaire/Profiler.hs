{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes      #-}

module Vaultaire.Profiler
     ( Profiler
     , ProfilerArgs
     , ProfilingEnv
     , ProfilingInterface(..)
     , Period
     , startProfiler
     , noProfiler
     , hasProfiler )
where

import           Control.Applicative
import           Control.Concurrent
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import qualified Data.Map.Strict as M
import           Data.Monoid
import           Data.UnixTime
import           Network.URI
import           Pipes
import           Pipes.Concurrent
import           Pipes.Lift
import           Pipes.Parse (foldAll)
import           System.Log.Logger
import qualified System.ZMQ4 as Z

import           Vaultaire.Types

-- TODO
-- the profiler also needs to check shutdown periodically
-- if the profiler crashes, the worker shouldn't be pulled down
-- but if the worker dies, the profiler should die too

type PublishSock = Z.Socket Z.Pub

type Period     = Int

-- | A profile action, with access to the internal connections.
newtype Profiler a = Profiler (ReaderT ProfilingEnv IO a)
        deriving ( Functor, Applicative, Monad, MonadIO
                 , MonadReader ProfilingEnv )

-- | Use the environment to run a profiler action.
--   *NOTE* this is destructive w.r.t the environment, afterwards
--          the environment cannot be reused for another profiler.
--
runProfiler :: ProfilingEnv -> Profiler a -> IO a
runProfiler e (Profiler x) = do
    r <- runReaderT x e
    _ <- _seal e
    return r

startProfiler :: ProfilingEnv -> IO ()
startProfiler env@(ProfilingEnv{..}) =
    Z.withContext $ \ctx ->
      Z.withSocket ctx Z.Pub $ \sock -> do
        Z.connect   sock $ show _publish
        runProfiler env $ profile sock

-- | Interface exposed to worker threads so they can report to the profiler.
data ProfilingInterface = ProfilingInterface
   -- Dictionary of reporting functions.
   -- without profiling, they should become no-op's.
   { profCount :: MonadIO m => TeleMsgType -> Origin -> m ()
   , profTime  :: MonadIO m => TeleMsgType -> Origin -> m r -> m r }

-- | Arguments needed to be specified by the user for profiling
type ProfilerArgs = (String, URI, Period)

-- | Profiling environment.
data ProfilingEnv = ProfilingEnv
   { _aname     :: AgentID         -- ^ Identifiable name for this daemon
   , _publish   :: URI             -- ^ Broker for telemetrics
   , _bound     :: Int             -- ^ Max telemetric messages from worker per period
   , _sleep     :: Int             -- ^ Period
   , _outchan   :: Output TeleMsg
   , _inchan    :: Input TeleMsg
   , _seal      :: IO () }

noProfiler :: (ProfilingEnv, ProfilingInterface)
noProfiler
    = ( ProfilingEnv
            { _aname    = mempty
            , _publish  = nullURI
            , _bound    = 0
            , _sleep    = 0
            , _outchan  = Output { send = const $ return False}
            , _inchan   = Input  { recv = return Nothing }
            , _seal     = return () }
      , ProfilingInterface
            { profCount = const $ const $ return ()
            , profTime  = const $ const id } )

hasProfiler :: ProfilerArgs -> IO (ProfilingEnv, ProfilingInterface)
hasProfiler (name, broker, period) =  do
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
    return ( ProfilingEnv
                 { _aname    = n
                 , _publish  = broker
                 , _bound    = 1024
                 , _sleep    = period
                 , _outchan  = output
                 , _inchan   = input
                 , _seal     = liftIO $ atomically sealchan }
           , ProfilingInterface
                 { profCount = g output
                 , profTime  = h output } )

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

profile :: PublishSock -> Profiler ()
profile sock = do
    ProfilingEnv{..} <- ask

    -- Read at most N reports from the profiling channel (N = size of the channel)
    -- since new reports would still be coming in after we have commenced this operation.
    msgs <- aggregate $ fromInputUntil _bound _inchan
    _    <- mapM (mkResp _aname >=> send) msgs

    -- Sleep for <period>
    liftIO $ threadDelay _sleep

    where fromInputUntil n chan = evalStateP 0 $ do
            x      <- lift $ get
            when (x <= n) $ fromInput chan

          mkResp n msg = do
            t      <- liftIO $ getCurrentTimeNanoseconds
            return $ TeleResp t n msg

          send resp = liftIO $ Z.send sock [] $ toWire resp

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
