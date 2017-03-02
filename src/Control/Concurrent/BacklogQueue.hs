module Control.Concurrent.BacklogQueue
  ( BQueue
  , BQueueConf
  , bQueueDefaultSize
  , bQueueDefaultConf
  , bQueueConfSetLengthHook
  , newBQueue
  , readBQueue
  , tryReadBQueue
  , writeBQueue
  , writeManyBQueue
  , sizeBQueue
  , lengthBQueue
  , lengthTVarBQueue
  ) where

import ClassyPrelude
import qualified Control.Concurrent.BacklogQueue.STM as STM
import Data.Function ((&))

data BQueue a = BQueue { _bQueueSTM :: STM.BQueue a
                       , _bQueueLengthHook :: Maybe (Int -> Int -> IO ()) }

data BQueueConf = BQueueConf { _bQueueConfSize       :: Int
                             , _bQueueConfLengthHook :: Maybe (Int -> Int -> IO ()) }

bQueueDefaultSize :: Int
bQueueDefaultSize = STM.bQueueDefaultSize

bQueueDefaultConf :: BQueueConf
bQueueDefaultConf = BQueueConf { _bQueueConfSize       = bQueueDefaultSize
                               , _bQueueConfLengthHook = Nothing }

bQueueConfSetLengthHook :: (Int -> Int -> IO ()) -> BQueueConf -> BQueueConf 
bQueueConfSetLengthHook cb conf = conf { _bQueueConfLengthHook = Just cb }

lengthHook :: BQueue a -> Int -> Int -> IO ()
lengthHook queue n n' =
  case _bQueueLengthHook queue of
    Just hook -> hook n n'
    Nothing   -> return ()

newBQueue :: BQueueConf -> IO (BQueue a)
newBQueue conf = do
  let confSTM = STM.bQueueConfDefault & STM.bQueueConfSetSize (_bQueueConfSize conf)
  queueSTM <- atomically $ STM.newBQueue confSTM
  let queue = BQueue { _bQueueSTM        = queueSTM
                     , _bQueueLengthHook = _bQueueConfLengthHook conf }
  lengthHook queue 0 0
  return queue

readBQueue :: BQueue a -> IO a
readBQueue queue = do
  let queueSTM = _bQueueSTM queue
  (n, a) <- atomically $ do
    n' <- STM.lengthBQueue queueSTM
    a' <- STM.readBQueue queueSTM
    return (n', a')
  lengthHook queue n (n - 1)
  return a

tryReadBQueue :: BQueue a -> IO (Maybe a)
tryReadBQueue queue = do
  let queueSTM = _bQueueSTM queue
  (n, maybeA) <- atomically $ do
    n' <- STM.lengthBQueue queueSTM
    a' <- STM.tryReadBQueue queueSTM
    return (n', a')
  case maybeA of
    Just _  -> lengthHook queue n (n - 1)
    Nothing -> return ()
  return maybeA

writeBQueue :: BQueue a -> a -> IO ()
writeBQueue queue a = do
  let queueSTM = _bQueueSTM queue
  n <- atomically $ do
    n' <- STM.lengthBQueue queueSTM
    STM.writeBQueue queueSTM a
    return n'
  lengthHook queue n (n + 1)

writeManyBQueue :: BQueue a -> [a] -> IO [a]
writeManyBQueue queue as = do
  let queueSTM = _bQueueSTM queue
  (n, asRemainder) <- atomically $ do
    n'           <- STM.lengthBQueue queueSTM
    asRemainder' <- STM.writeManyBQueue queueSTM as
    return (n', asRemainder')
  let nWritten = length as - length asRemainder
  lengthHook queue n (n + nWritten)
  return asRemainder

sizeBQueue :: BQueue a -> Int
sizeBQueue = STM.sizeBQueue . _bQueueSTM

lengthBQueue :: BQueue a -> IO Int
lengthBQueue = atomically . STM.lengthBQueue . _bQueueSTM

lengthTVarBQueue :: BQueue a -> TVar Int
lengthTVarBQueue = STM.lengthTVarBQueue . _bQueueSTM
