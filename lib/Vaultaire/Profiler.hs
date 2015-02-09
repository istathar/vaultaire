{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Vaultaire.Profiler
     ( Profiler
     , ProfilerArgs
     , ProfilingEnv
     , ProfilingInterface(..)
     , Period
     , startProfiler
     , noProfiler
     , hasProfiler
     , quickMedian)
where

import           Control.Applicative
import           Control.Concurrent hiding (yield)
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import qualified Data.Map.Strict as M
import qualified Data.List as L
import           Data.Maybe
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
--   in case of no profiling, these functions should be basically noops.
--
data ProfilingInterface = ProfilingInterface
    { -- Reporting functions, they will perform the necessary measurements
      -- and send them to the profiler.
      profCount   :: MonadIO m => TeleMsgType -> Origin -> Int -> m ()
    , profTime    :: MonadIO m => TeleMsgType -> Origin -> m r -> m r
      -- Raw measurement and sending functions.
    , measureTime :: MonadIO m => m r -> m (r, Word64)
    , report      :: MonadIO m => TeleMsgType -> Origin -> Word64 -> m () }

-- | Arguments needed to be specified by the user for profiling
--   (name, publishing port, period, shutdown signal)
--
type ProfilerArgs = (String, URI, Period, MVar ())

-- | Profiling environment.
data ProfilingEnv = ProfilingEnv
    { _aname     :: AgentID         -- ^ Identifiable name for this daemon
    , _publish   :: URI             -- ^ Broker for telemetrics
    , _bound     :: Int             -- ^ Max telemetric messages from worker per period
    , _sleep     :: Int             -- ^ Period, in milliseconds
    , _output    :: Output ChanMsg  -- ^ Send to the profiler via this output
    , _input     :: Input  ChanMsg  -- ^ Receive messages sent to the profiler via this input
    , _seal      :: IO ()           -- ^ Seal the profiler chan
    , _shutdown  :: MVar ()         -- ^ Shutdown signal
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
            , _seal     = return ()
            -- This is fine because this MVar will never be read
            -- the profiling environment accessors are not exported.
            , _shutdown = undefined }
      , ProfilingInterface
            { profCount   = const $ const $ const $ return ()
            , profTime    = const $ const id
            , measureTime = (>>= return . (,0)) . id
            , report      = const $ const $ const $ return () } )

hasProfiler :: ProfilerArgs -> IO (ProfilingEnv, ProfilingInterface)
hasProfiler (name, broker, period, quit) =  do
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
                 , _seal     = liftIO $ atomically sealchan
                 , _shutdown = quit }
           , ProfilingInterface
                 { profCount   = sendCount   output
                 , profTime    = sendElapsed output
                 , measureTime = elapsed
                 , report      = sendIt      output } )

    where sendCount output teletype origin count = do
            -- sending to the profiler shouldn't fail (as the buffer is @Newest@)
            -- but if it does there is nothing the worker could do about it
            _ <- liftIO $ atomically $ send output
               $ Tele $ TeleMsg origin teletype $ fromIntegral count
            return ()

          sendIt output teletype origin payload = do
            _ <- liftIO $ atomically $ send output
               $ Tele $ TeleMsg origin teletype payload
            return ()

          elapsed act = do
            !t1 <- liftIO $ getUnixTime
            r   <- act
            !t2 <- liftIO $ getUnixTime
            return (r, diffTimeInMs $ diffUnixTime t2 t1)

          sendElapsed output teletype origin act = do
            (r, t) <- elapsed act
            _      <- liftIO $ atomically $ send output $ Tele
                   $  TeleMsg origin teletype t
            return r

          diffTimeInMs :: UnixDiffTime -> Word64
          diffTimeInMs u
            = let secInMilliSec  = (raw $ udtSeconds u) * 1000
                  uSecInMilliSec = (udtMicroSeconds u) `div` 1000
              in  fromIntegral $ secInMilliSec + fromIntegral uSecInMilliSec
          raw (CTime x) = x

profile :: PublishSock -> Profiler ()
profile sock = do
    ProfilingEnv{..} <- ask

    done <- isJust <$> liftIO (tryReadMVar _shutdown)
    unless done $ do
      -- Read at most N reports from the profiling channel (N = size of the channel)
      -- since new reports would still be coming in after we have commenced this operation.
      _    <- liftIO $ atomically $ send _output Barrier
      msgs <- aggregate $ fromInputUntil _bound _input
      _    <- mapM (mkResp _aname >=> pub) msgs

      -- Sleep for <period> milliseconds
      liftIO $ milliDelay _sleep
      profile sock

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
    (\acc x -> M.insertWith (go $ _type x)
                            (_origin x, _type x)
                            (1, [_payload x])
                            acc)
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
          extract k@(_, WriterSimplePoints      ) = total  k
          extract k@(_, WriterExtendedPoints    ) = total  k
          extract k@(_, WriterRequest           ) = total  k
          extract k@(_, WriterRequestLatency    ) = median k
          extract k@(_, WriterCephLatency       ) = median k
          extract k@(_, ReaderSimplePoints      ) = total  k
          extract k@(_, ReaderExtendedPoints    ) = total  k
          extract k@(_, ReaderRequest           ) = total  k
          extract k@(_, ReaderRequestLatency    ) = median k
          extract k@(_, ReaderCephLatency       ) = median k
          extract k@(_, ContentsEnumerate       ) = total  k
          extract k@(_, ContentsUpdate          ) = total  k
          extract k@(_, ContentsEnumerateLatency) = median k
          extract k@(_, ContentsUpdateLatency   ) = median k
          extract k@(_, ContentsEnumerateCeph   ) = median k
          extract k@(_, ContentsUpdateCeph      ) = median k
          count (c1, x)  (c2, _)   = (c1 + c2, x)
          keep  (c1, vs) (c2, vs') = (c1 + c2, vs' ++ vs)
          total  k                 = msg k <$> fst
          median k                 = msg k <$> mid
          msg   (x,y)    z         = TeleMsg x y z
          -- median is:
          -- - middle element if list is odd
          -- - average of the two middles if the list is even
          -- these lists of latency values should not be too long, this code
          -- can be optimised if there's a performance issue with the profiler.
          -- in particular, the median can be calculated in O(n).
          mid :: (Word64, [Word64]) -> Word64
          mid   (c, v)    = let len = (fromIntegral c :: Int)
                             in fromIntegral $ quickMedian len v

quickselect :: Ord a => Int -> [a] -> a
quickselect k (x:xs) | k < pl    = quickselect k leftPart
                     | k > pl    = quickselect (k-pl-1) rightPart
                     | otherwise = x
  where
    (leftPart, rightPart) = L.partition (< x) xs
    pl                    = length leftPart

quickMedian :: (Ord a, Integral a) => Int -> [a] -> a
quickMedian 0 [] = 0
quickMedian len xs
    | len `mod` 2 == 0 =  (quickselect (len `div` 2 - 1) xs
                         + quickselect (len `div` 2) xs)
                           `div` 2
    | otherwise        = quickselect (len `div` 2) xs

milliDelay :: Int -> IO ()
milliDelay = threadDelay . (*1000)
