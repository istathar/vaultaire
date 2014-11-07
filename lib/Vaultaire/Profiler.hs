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
import           Control.Concurrent.MVar
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import qualified Data.Map.Strict as M
import           Data.UnixTime
import           Data.Monoid
import           GHC.Int
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
        runDaemon args (profile args period sock)

noProfiler :: Profiler
noProfiler =  Profiler
    { aname     = mempty
    , outchan   = Output { send = const $ return False}
    , inchan    = Input  { recv = return Nothing }
    , seal      = return ()
    , profCount = const $ return ()
    , profTime  = const id }

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
           , outchan   = output
           , inchan    = input
           , seal      = liftIO $ atomically sealchan
           , profCount = g output
           , profTime  = h output }
    where g o t     = liftIO $ do
                        _ <- atomically (send o $ TeleMsg t 1)
                        return ()
          h o t act = do t1 <- liftIO $ getUnixTime
                         r  <- act
                         t2 <- liftIO $ getUnixTime
                         _  <- liftIO $ atomically $ send o
                            $  TeleMsg t $ fromIntegral
                            $  udtMicroSeconds
                            $  diffUnixTime t1 t2
                         return r

profileCount :: TeleMsgType -> Daemon ()
profileCount t = do
    (_, prof) <- ask
    profCount prof t

profileTime :: TeleMsgType -> Daemon r -> Daemon r
profileTime  t act = do
    (_, prof) <- ask
    profTime prof t act

profile :: DaemonArgs -> Period -> PublishSock -> Daemon ()
profile args period sock = do
    (_, c) <- ask
    -- Read at most N reports from the profiling channel (N = size of the channel)
    -- since new reports would still be coming in after we have commenced this operation.
    msgs <- aggregate $ fromInputUntil size c
    mapM (mkResp c >=> publish sock) msgs
    -- Sleep for <period>
    liftIO $ threadDelay period
    where fromInputUntil n c = evalStateP 0 $ do
            x      <- lift $ get
            when (x <= n) $ fromInput (inchan c)
          mkResp c msg = do
            t      <- liftIO $ getCurrentTimeNanoseconds
            return $ TeleResp origin t (aname c) msg
          size    = undefined
          origin  = undefined

publish :: PublishSock -> TeleResp -> Daemon ()
publish sock resp = do
    (_, c) <- ask
    liftIO $ Z.send sock [] $ toWire resp

-- | Aggregate telemetric reports, guaranteed to process only N reports
--   see @report@.
--
aggregate :: Monad m => Producer TeleMsg m () -> m [TeleMsg]
aggregate = evalStateT $ foldAll
  (\acc x -> M.insertWith (go $ _type x) (_type x) (1, _payload x) acc)
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
        extract k@(WriterSimplePoints      ) = msg k <$> (fst                )
        extract k@(WriterExtendedPoints    ) = msg k <$> (fst                )
        extract k@(WriterRequest           ) = msg k <$> (fst                )
        extract k@(WriterRequestLatency    ) = msg k <$> (div <$> snd <*> fst)
        extract k@(WriterCephLatency       ) = msg k <$> (div <$> snd <*> fst)
        extract k@(ReaderSimplePoints      ) = msg k <$> (fst                )
        extract k@(ReaderExtendedPoints    ) = msg k <$> (fst                )
        extract k@(ReaderRequest           ) = msg k <$> (fst                )
        extract k@(ReaderRequestLatency    ) = msg k <$> (div <$> snd <*> fst)
        extract k@(ReaderCephLatency       ) = msg k <$> (div <$> snd <*> fst)
        extract k@(ContentsEnumerate       ) = msg k <$> (fst                )
        extract k@(ContentsUpdate          ) = msg k <$> (fst                )
        extract k@(ContentsEnumerateLatency) = msg k <$> (div <$> snd <*> fst)
        extract k@(ContentsUpdateLatency   ) = msg k <$> (div <$> snd <*> fst)
        extract k@(ContentsEnumerateCeph   ) = msg k <$> (div <$> snd <*> fst)
        extract k@(ContentsUpdateCeph      ) = msg k <$> (div <$> snd <*> fst)
        count (c1, _)  (c2, _)  = (c1 + c2, 0)
        keep  (c1, v1) (c2, v2) = (c1 + c2, v1 + v2)
        msg = TeleMsg
