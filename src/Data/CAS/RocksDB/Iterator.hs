-- | This code is mostly from serokell/rocksdb-haskell's Iterator module.
--   The main difference is the way that options are handled. The original code
--   didn't let me alter any options that didn't have bindings. It also had the
--   ability to leak the underlying `ReadOptions` pointer.
{-# language ScopedTypeVariables #-}
{-# language RankNTypes #-}

module Data.CAS.RocksDB.Iterator where

import           Control.Exception            (bracket)
import           Control.Monad                (when)
import           Data.ByteString              (ByteString)
import           Data.Maybe                   (catMaybes)
import           Foreign
import           Foreign.C.Error              (throwErrnoIfNull)
import           Foreign.C.String
import           Foreign.C.Types

import           Database.RocksDB.C
import           Database.RocksDB.Internal hiding (mkCReadOpts, withCReadOpts)
import           Database.RocksDB.Types hiding (ReadOptions)

import qualified Data.ByteString              as BS
import qualified Data.ByteString.Char8        as BC
import qualified Data.ByteString.Unsafe       as BU

createIter :: DB -> ReadOptionsPtr -> IO IteratorPtr
createIter (DB db_ptr _) opts_ptr =
    throwErrnoIfNull "create_iterator" $ c_rocksdb_create_iterator db_ptr opts_ptr

-- | Run an action with an 'IteratorPtr'
withIter :: DB -> ReadOptionsPtr -> (IteratorPtr -> IO a) -> IO a
withIter db opts_ptr =
    bracket (createIter db opts_ptr) c_rocksdb_iter_destroy

-- | An iterator is either positioned at a key/value pair, or not valid. This
-- function returns /true/ iff the iterator is valid.
iterValid :: IteratorPtr -> IO Bool
iterValid iter_ptr = do
    x <- c_rocksdb_iter_valid iter_ptr
    return (x /= 0)

-- | Position at the first key in the source that is at or past target. The
-- iterator is /valid/ after this call iff the source contains an entry that
-- comes at or past target.
iterSeek :: IteratorPtr -> ByteString -> IO ()
iterSeek iter_ptr key =
    BU.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
        c_rocksdb_iter_seek iter_ptr key_ptr (intToCSize klen)

-- | Position at the first key in the source. The iterator is /valid/ after this
-- call iff the source is not empty.
iterFirst :: IteratorPtr -> IO ()
iterFirst iter_ptr = c_rocksdb_iter_seek_to_first iter_ptr

-- | Position at the last key in the source. The iterator is /valid/ after this
-- call iff the source is not empty.
iterLast :: IteratorPtr -> IO ()
iterLast iter_ptr = c_rocksdb_iter_seek_to_last iter_ptr

-- | Moves to the next entry in the source. After this call, 'iterValid' is
-- /true/ iff the iterator was not positioned at the last entry in the source.
--
-- If the iterator is not valid, this function does nothing. Note that this is a
-- shortcoming of the C API: an 'iterPrev' might still be possible, but we can't
-- determine if we're at the last or first entry.
iterNext :: IteratorPtr -> IO ()
iterNext iter_ptr = do
    valid <- c_rocksdb_iter_valid iter_ptr
    when (valid /= 0) $ c_rocksdb_iter_next iter_ptr

-- | Moves to the previous entry in the source. After this call, 'iterValid' is
-- /true/ iff the iterator was not positioned at the first entry in the source.
--
-- If the iterator is not valid, this function does nothing. Note that this is a
-- shortcoming of the C API: an 'iterNext' might still be possible, but we can't
-- determine if we're at the last or first entry.
iterPrev :: IteratorPtr -> IO ()
iterPrev iter_ptr = do
    valid <- c_rocksdb_iter_valid iter_ptr
    when (valid /= 0) $ c_rocksdb_iter_prev iter_ptr

-- | Return the key for the current entry if the iterator is currently
-- positioned at an entry, ie. 'iterValid'.
iterKey :: IteratorPtr -> IO (Maybe ByteString)
iterKey = flip iterString c_rocksdb_iter_key

-- | Return the value for the current entry if the iterator is currently
-- positioned at an entry, ie. 'iterValid'.
iterValue :: IteratorPtr -> IO (Maybe ByteString)
iterValue = flip iterString c_rocksdb_iter_value

-- | Return the current entry as a pair, if the iterator is currently positioned
-- at an entry, ie. 'iterValid'.
iterEntry :: IteratorPtr -> IO (Maybe (ByteString, ByteString))
iterEntry iter = do
    mkey <- iterKey iter
    mval <- iterValue iter
    return $ (,) <$> mkey <*> mval

-- | Check for errors
--
-- Note that this captures somewhat severe errors such as a corrupted database.
iterGetError :: IteratorPtr -> IO (Maybe ByteString)
iterGetError iter_ptr =
    alloca $ \err_ptr -> do
        poke err_ptr nullPtr
        c_rocksdb_iter_get_error iter_ptr err_ptr
        erra <- peek err_ptr
        if erra == nullPtr
            then return Nothing
            else do
                err <- peekCString erra
                return . Just . BC.pack $ err

-- | Map a function over an iterator, advancing the iterator forward and
-- returning the value. The iterator should be put in the right position prior
-- to calling the function.
--
-- Note that this function accumulates the result strictly, ie. it reads all
-- values into memory until the iterator is exhausted. This is most likely not
-- what you want for large ranges. You may consider using conduits instead, for
-- an example see: <https://gist.github.com/adc8ec348f03483446a5>
mapIter :: (IteratorPtr -> IO a) -> IteratorPtr -> IO [a]
mapIter f iter@iter_ptr = go []
  where
    go acc = do
        valid <- c_rocksdb_iter_valid iter_ptr
        if valid == 0
            then return acc
            else do
                val <- f iter
                ()  <- c_rocksdb_iter_next iter_ptr
                go (val : acc)

-- | Return a list of key and value tuples from an iterator. The iterator
-- should be put in the right position prior to calling this with the iterator.
--
-- See strictness remarks on 'mapIter'.
iterItems :: IteratorPtr -> IO [(ByteString, ByteString)]
iterItems iter = catMaybes <$> mapIter iterEntry iter

-- | Return a list of key from an iterator. The iterator should be put
-- in the right position prior to calling this with the iterator.
--
-- See strictness remarks on 'mapIter'
iterKeys :: IteratorPtr -> IO [ByteString]
iterKeys iter = catMaybes <$> mapIter iterKey iter

-- | Return a list of values from an iterator. The iterator should be put
-- in the right position prior to calling this with the iterator.
--
-- See strictness remarks on 'mapIter'
iterValues :: IteratorPtr -> IO [ByteString]
iterValues iter = catMaybes <$> mapIter iterValue iter

--
-- Internal
--

iterString :: IteratorPtr
           -> (IteratorPtr -> Ptr CSize -> IO CString)
           -> IO (Maybe ByteString)
iterString iter_ptr f = do
    valid <- c_rocksdb_iter_valid iter_ptr
    if valid == 0
    then return Nothing
    else
        alloca $ \len_ptr -> do
            ptr <- f iter_ptr len_ptr
            if ptr == nullPtr
            then return Nothing
            else do
                len <- peek len_ptr
                Just <$> BS.packCStringLen (ptr, cSizeToInt len)
