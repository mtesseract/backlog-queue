module Control.Concurrent.BacklogQueue.STM
  ( BQueue
  , BQueueConf
  , bQueueDefaultSize
  , bQueueConfDefault
  , bQueueConfSetSize
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
import Control.Concurrent.STM (retry)

data BQueue a = BQueue { _bQueueSize       :: Int
                       , _bQueue           :: TBQueue a
                       , _bQueueLength     :: TVar Int
                       , _bQueueLengthHook :: Maybe (Int -> Int -> STM ()) }

data BQueueConf = BQueueConf { _bQueueConfSize       :: Int
                             , _bQueueConfLengthHook :: Maybe (Int -> Int -> STM ()) }

bQueueDefaultSize :: Int
bQueueDefaultSize = 1024

bQueueConfDefault :: BQueueConf
bQueueConfDefault = BQueueConf { _bQueueConfSize       = bQueueDefaultSize
                               , _bQueueConfLengthHook = Nothing }

bQueueConfSetSize :: Int -> BQueueConf -> BQueueConf 
bQueueConfSetSize n conf = conf { _bQueueConfSize = n }

bQueueConfSetLengthHook :: (Int -> Int -> STM ()) -> BQueueConf -> BQueueConf 
bQueueConfSetLengthHook cb conf = conf { _bQueueConfLengthHook = Just cb }

lengthHook :: BQueue a -> Int -> Int -> STM ()
lengthHook q n n' =
  case _bQueueLengthHook q of
    Just hook -> hook n n'
    Nothing   -> return ()

newBQueue :: BQueueConf -> STM (BQueue a)
newBQueue conf = do
  q <- BQueue <$> pure (_bQueueConfSize conf)
              <*> newTBQueue (_bQueueConfSize conf)
              <*> newTVar 0
              <*> pure (_bQueueConfLengthHook conf)
  lengthHook q 0 0
  return q

readBQueue :: BQueue a -> STM a
readBQueue queue = do
  len <- readTVar (_bQueueLength queue)
  let len' = len - 1
  a <- readTBQueue (_bQueue queue)
  writeTVar (_bQueueLength queue) len'
  lengthHook queue len len'
  return a

tryReadBQueue :: BQueue a -> STM (Maybe a)
tryReadBQueue queue = do
  len <- readTVar (_bQueueLength queue)
  maybeA <- tryReadTBQueue (_bQueue queue)
  case maybeA of
    Just a  -> do let len' = len - 1
                  writeTVar (_bQueueLength queue) len'
                  lengthHook queue len len'
    Nothing -> return ()
  return maybeA

writeBQueue :: BQueue a -> a -> STM ()
writeBQueue queue a = do
  len <- readTVar (_bQueueLength queue)
  if len < _bQueueSize queue
    then do writeTBQueue (_bQueue queue) a
            modifyTVar (_bQueueLength queue) (+ 1)
    else retry

writeManyBQueue :: BQueue a -> [a] -> STM [a]
writeManyBQueue queue as = do
  len <- readTVar (_bQueueLength queue)
  let nCanWrite = _bQueueSize queue - len
      (asToWrite, asRemainder) = splitAt nCanWrite as
  mapM_ (writeTBQueue (_bQueue queue)) asToWrite
  modifyTVar (_bQueueLength queue) (+ (length asToWrite))
  return asRemainder

sizeBQueue :: BQueue a -> Int
sizeBQueue = _bQueueSize

lengthBQueue :: BQueue a -> STM Int
lengthBQueue = readTVar . _bQueueLength

lengthTVarBQueue :: BQueue a -> TVar Int
lengthTVarBQueue = _bQueueLength
