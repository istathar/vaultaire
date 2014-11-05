module Vaultaire.Profiler
     ( Period
     , startProfiler )
where

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import qualified Data.Map.Strict as M
import           Data.Word
import           Pipes
import           Pipes.Concurrent
import           Pipes.Lift
import           Pipes.Parse (foldAll)
import           System.Log.Logger

import           Vaultaire.Daemon
import           Vaultaire.Types


startProfiler :: DaemonArgs -> Period -> IO ()
startProfiler args = undefined

-- | Listen on profiling channel for reports from other daemons,
--   aggregate them (count/average/etc) and construct a telemetric response.
--
report :: DaemonArgs -> Period -> Daemon ()
report args period = do
    -- Read at most N reports from the profiling channel (N = size of the channel)
    -- since new reports would still be coming in after we have commenced this operation.
    msgs <- aggregate $ fromInputUntil size
    mapM (publish . mkResp) msgs
    -- Sleep for <period>
    liftIO $ threadDelay period
    where fromInputUntil n = evalStateP 0 $ do
            x <- lift $ get
            c <- lift $ ask
            when (x <= n)
               $ maybe (return ()) fromInput
                       (internalChanIn c)
          mkResp msg = do
            t <- getCurrentTimeNanoseconds
            return $ TeleResp origin t msg
          publish = undefined
          size    = undefined
          origin  = undefined

-- | Aggregate telemetric reports, guaranteed to process only N reports
--   see @report@.
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
