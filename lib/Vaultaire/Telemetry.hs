module Vaultaire.Telemetry
     ( startTelemetry
     , telemetryChan )
where

import           Control.Concurrent.MVar
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import qualified Data.Map.Strict as M
import           Data.Word
import           Pipes
import           Pipes.Parse

import           Vaultaire.Daemon
import           Vaultaire.Types


-- | Start a writer daemon, runs until shutdown.
startTelemetry
  :: String           -- ^ Broker
  -> Maybe ByteString -- ^ Username for Ceph
  -> ByteString       -- ^ Pool name for Ceph
  -> Int              -- ^ Maximum frequency allowed
  -> MVar ()          -- ^ Shutdown signal
  -> IO ()
startTelemetry broker user pool min_period shutdown
  = handleMessages broker user pool shutdown (report min_period)

telemetryChan :: Daemon (Input TeleMsg)
telemetryChan = inchan <$> ask

handleRequest :: Int -> Message -> Daemon ()
handleRequest min_period (Message reply_f origin payload) = case fromWire payload of
    Right (TeleRequest period agent) -> do
          if period < min_period
          then liftIO $ errorM "Telemetry.handleRequest"
                      $  "requested period "                  ++ show period
                      ++ " is less than the minimum allowed " ++ show min_period
          else ask >>= report origin reply_f period . inchan

    Left e -> liftIO $ errorM "Telemetry.handleRequest" $ "failed to decode request: " ++ show e

-- | Listen on profiling channel for reports from other daemons,
--   aggregate them (count/average/etc) and construct a telemetric response.
--
report :: Origin -> ReplyF -> Int -> Input TeleMsg -> Daemon ()
report org reply_f period chan = undefined
    -- Read at most N reports from the profiling channel (N = size of the channel)
    -- since new reports would still be coming in after we have commenced this operation.
    msgs <- aggregate $ fromInputUntil undefined
    mapM (reply_f . mkResp) msgs
    where fromInputUntil n = evalStateP 0 $ do
            x <- lift $ get
            if x <= n then fromInput chan else return ()
          mkResp msg = do
            t <- getCurrentTimeNanoseconds
            return $ TeleResp org t msg

-- | Aggregate telemetric reports.
--
aggregate :: Monad m => Producer TeleMsg m () -> m [TeleMsg]
aggregate = evalStateT $ foldAll
  (\acc x -> M.insertWith (go $ _type x) (_aid x, _type x) (1, _payload x) acc)
  (M.empty)
  (map (uncurry extract) . M.toList)
  where go WriterPoints             = count
        go WriterRequest            = count
        go WriterRequestLatency     = keep
        go WriterCephLatency        = keep
        go ReaderPoints             = count
        go ReaderRequest            = count
        go ReaderRequestLatency     = keep
        go ReaderCephLatency        = keep
        go ContentsEnumerate        = count
        go ContentsUpdate           = count
        go ContentsEnumerateLatency = keep
        go ContentsUpdateLatency    = keep
        go ContentsEnumerateCeph    = keep
        go ContentsUpdateCeph       = keep
        extract k@(_, WriterPoints            ) = msg k <$> (fst                )
        extract k@(_, WriterRequest           ) = msg k <$> (fst                )
        extract k@(_, WriterRequestLatency    ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, WriterCephLatency       ) = msg k <$> (div <$> snd <*> fst)
        extract k@(_, ReaderPoints            ) = msg k <$> (fst                )
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
        msg  (x,y) z = TeleMsg x y z
