{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

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
import           Control.Concurrent hiding (yield)
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import qualified Data.Map.Strict as M
import           Data.Monoid
import           Data.UnixTime
import           Data.Word
import           Foreign.C.Types (CTime(..))
import           Network.URI
import           Pipes
import           Pipes.Lift
import           Pipes.Concurrent
import           Pipes.Parse (foldAll)
import           System.Log.Logger
import qualified System.ZMQ4 as Z

import           Vaultaire.Types

-- | The profiler will publish on this socket.
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
   { profCountN :: MonadIO m => TeleMsgType -> Origin -> Int -> m ()
   , profTime   :: MonadIO m => TeleMsgType -> Origin -> m r -> m r }

-- | Arguments needed to be specified by the user for profiling
--   (name, publishing port, period)
type ProfilerArgs = (String, URI, Period)

-- | Profiling environment.
data ProfilingEnv = ProfilingEnv
   { _aname     :: AgentID         -- ^ Identifiable name for this daemon
   , _publish   :: URI             -- ^ Broker for telemetrics
   , _bound     :: Int             -- ^ Max telemetric messages from worker per period
   , _sleep     :: Int             -- ^ Period, in milliseconds
   , _output    :: Output ChanMsg  -- ^ Send to the profiler via this output
   , _input     :: Input  ChanMsg  -- ^ Receive messages sent to the profiler via this input
   , _seal      :: IO ()           -- ^ Seal the profiler chan
   }

-- | Values that can be sent to the profiling channel.
--
data ChanMsg = Barrier
             | Tele TeleMsg
             deriving Show

noProfiler :: (ProfilingEnv, ProfilingInterface)
noProfiler
  = ( ProfilingEnv
          { _aname    = mempty
          , _publish  = nullURI
          , _bound    = 0
          , _sleep    = 0
          , _output   = Output { send = const $ return False   }
          , _input    = Input  { recv =         return Nothing }
          , _seal     = return () }
    , ProfilingInterface
          { profCountN = const $ const $ const $ return ()
          , profTime   = const $ const id } )

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
  -- This means the profiler will lose precision but not have
  -- an impact on performance if there is too much activity.
  (output, input, sealchan) <- spawn' $ Newest 1024
  return ( ProfilingEnv
               { _aname    = n
               , _publish  = broker
               , _bound    = 1024
               , _sleep    = period
               , _output   = output
               , _input    = input
               , _seal     = liftIO $ atomically sealchan }
         , ProfilingInterface
               { profCountN = f output
               , profTime   = h output } )

  where f output teletype origin count = liftIO $ do
          _ <- atomically (send output $ Tele $ TeleMsg origin teletype $ fromIntegral count)
          return ()
        h output teletype origin act = do
          !t1 <- liftIO $ getUnixTime
          r   <- act
          !t2 <- liftIO $ getUnixTime
          _  <- liftIO $ atomically $ send output $ Tele
             $  TeleMsg origin teletype
             $  diffTimeInMs
             $  diffUnixTime t2 t1
          return r
        diffTimeInMs :: UnixDiffTime -> Word64
        diffTimeInMs u
          = let secInMilliSec  = (raw $ udtSeconds u) * 1000
                uSecInMilliSec = (udtMicroSeconds u) `div` 1000
            in  fromIntegral $ secInMilliSec + fromIntegral uSecInMilliSec
        raw (CTime x) = x

profile :: PublishSock -> Profiler ()
profile sock = forever $ do
  ProfilingEnv{..} <- ask

  -- Read at most N reports from the profiling channel (N = size of the channel)
  -- since new reports would still be coming in after we have commenced this operation.
  _    <- liftIO $ atomically $ send _output Barrier
  msgs <- aggregate $ fromInputUntil _bound _input
  _    <- mapM (mkResp _aname >=> pub) msgs

  -- Sleep for <period> milliseconds
  liftIO $ milliDelay _sleep

  where mkResp :: MonadIO m => AgentID -> TeleMsg -> m TeleResp
        mkResp n msg = do
          t      <- liftIO $ getCurrentTimeNanoseconds
          return $ TeleResp t n msg

        pub :: TeleResp -> Profiler ()
        pub resp = liftIO $ Z.send sock [] $ toWire resp

-- | Reads from input until we either hit a barrier or reach the cap.
--   Like pipes-concurrency's @fromInput@ but non-blocking.
--
fromInputUntil :: MonadIO m => Int -> Input ChanMsg -> Producer TeleMsg m ()
fromInputUntil n chan = evalStateP 0 go
  where go = do
          x  <- lift get
          when (x <= n) $ do
            a <- liftIO $ atomically $ recv chan
            case a of Just (Tele t) -> yield t >> lift (put (x + 1)) >> go
                      _             -> return ()

-- | Aggregate telemetric reports, guaranteed to process only N reports.
--
--   *NOTE* Technically we do not need to report number of requests received,
--          since we can just count the number of latency samples,
--          but to keep things simple and modular we will leave them separate.
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

milliDelay :: Int -> IO ()
milliDelay = threadDelay . (*1000)
