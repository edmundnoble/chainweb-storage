{-# LANGUAGE CPP, ForeignFunctionInterface, EmptyDataDecls #-}
-- |
-- Module      : Database.RocksDB.C
-- Copyright   : (c) 2012-2013 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : kim.altintop@gmail.com
-- Stability   : experimental
-- Portability : non-portable
--

module Database.RocksDB.C where

import Foreign
import Foreign.C.Types
import Foreign.C.String

data RocksDB
data LCache
data LIterator
data LLogger
data LOptions
data LReadOptions
data LSnapshot
data LWriteBatch
data LWriteOptions
data LFilterPolicy

type RocksDBPtr      = Ptr RocksDB
type CachePtr        = Ptr LCache
type IteratorPtr     = Ptr LIterator
type LoggerPtr       = Ptr LLogger
type OptionsPtr      = Ptr LOptions
type ReadOptionsPtr  = Ptr LReadOptions
type SnapshotPtr     = Ptr LSnapshot
type WriteBatchPtr   = Ptr LWriteBatch
type WriteOptionsPtr = Ptr LWriteOptions
type FilterPolicyPtr = Ptr LFilterPolicy

type DBName = CString
type ErrPtr = Ptr CString
type Key    = CString
type Val    = CString

newtype CompressionOpt = CompressionOpt { compressionOpt :: CInt }
  deriving (Eq, Show)

noCompression :: CompressionOpt
noCompression = CompressionOpt 0
snappyCompression :: CompressionOpt
snappyCompression = CompressionOpt 1
zlibCompression :: CompressionOpt
zlibCompression = CompressionOpt 2
bz2Compression :: CompressionOpt
bz2Compression = CompressionOpt 3
lz4Compression :: CompressionOpt
lz4Compression = CompressionOpt 4
lz4hcCompression :: CompressionOpt
lz4hcCompression = CompressionOpt 5

foreign import ccall safe "rocksdb\\c.h rocksdb_open"
  c_rocksdb_open :: OptionsPtr -> DBName -> ErrPtr -> IO RocksDBPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_close"
  c_rocksdb_close :: RocksDBPtr -> IO ()


foreign import ccall safe "rocksdb\\c.h rocksdb_put"
  c_rocksdb_put :: RocksDBPtr
                -> WriteOptionsPtr
                -> Key -> CSize
                -> Val -> CSize
                -> ErrPtr
                -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_delete"
  c_rocksdb_delete :: RocksDBPtr
                   -> WriteOptionsPtr
                   -> Key -> CSize
                   -> ErrPtr
                   -> IO ()

foreign import ccall unsafe "chainweb\\rocksdb.h rocksdb_delete_range"
    rocksdb_delete_range :: RocksDBPtr
                         -> WriteOptionsPtr
                         -> Key {- min key -}
                         -> CSize {- min key length -}
                         -> Key {- max key -}
                         -> CSize {- max key length -}
                         -> ErrPtr
                         -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_compact_range"
    rocksdb_compact_range :: RocksDBPtr
                          -> Key {- min key -}
                          -> CSize {- min key length -}
                          -> Key {- max key -}
                          -> CSize {- max key length -}
                          -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_write"
  c_rocksdb_write :: RocksDBPtr
                  -> WriteOptionsPtr
                  -> WriteBatchPtr
                  -> ErrPtr
                  -> IO ()

-- | Returns NULL if not found. A malloc()ed array otherwise. Stores the length
-- of the array in *vallen.
foreign import ccall safe "rocksdb\\c.h rocksdb_get"
  c_rocksdb_get :: RocksDBPtr
                -> ReadOptionsPtr
                -> Key -> CSize
                -> Ptr CSize        -- ^ value length
                -> ErrPtr
                -> IO CString

foreign import ccall safe "rocksdb\\c.h rocksdb_create_snapshot"
  c_rocksdb_create_snapshot :: RocksDBPtr -> IO SnapshotPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_release_snapshot"
  c_rocksdb_release_snapshot :: RocksDBPtr -> SnapshotPtr -> IO ()

-- | Returns NULL if property name is unknown. Else returns a pointer to a
-- malloc()-ed null-terminated value.
foreign import ccall safe "rocksdb\\c.h rocksdb_property_value"
  c_rocksdb_property_value :: RocksDBPtr -> CString -> IO CString

foreign import ccall safe "rocksdb\\c.h rocksdb_approximate_sizes"
  c_rocksdb_approximate_sizes :: RocksDBPtr
                              -> CInt                     -- ^ num ranges
                              -> Ptr CString -> Ptr CSize -- ^ range start keys (array)
                              -> Ptr CString -> Ptr CSize -- ^ range limit keys (array)
                              -> Ptr Word64               -- ^ array of approx. sizes of ranges
                              -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_destroy_db"
  c_rocksdb_destroy_db :: OptionsPtr -> DBName -> ErrPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_repair_db"
  c_rocksdb_repair_db :: OptionsPtr -> DBName -> ErrPtr -> IO ()


--
-- Iterator
--

foreign import ccall safe "rocksdb\\c.h rocksdb_create_iterator"
  c_rocksdb_create_iterator :: RocksDBPtr -> ReadOptionsPtr -> IO IteratorPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_destroy"
  c_rocksdb_iter_destroy :: IteratorPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_valid"
  c_rocksdb_iter_valid :: IteratorPtr -> IO CUChar

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_seek_to_first"
  c_rocksdb_iter_seek_to_first :: IteratorPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_seek_to_last"
  c_rocksdb_iter_seek_to_last :: IteratorPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_seek"
  c_rocksdb_iter_seek :: IteratorPtr -> Key -> CSize -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_next"
  c_rocksdb_iter_next :: IteratorPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_prev"
  c_rocksdb_iter_prev :: IteratorPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_key"
  c_rocksdb_iter_key :: IteratorPtr -> Ptr CSize -> IO Key

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_value"
  c_rocksdb_iter_value :: IteratorPtr -> Ptr CSize -> IO Val

foreign import ccall safe "rocksdb\\c.h rocksdb_iter_get_error"
  c_rocksdb_iter_get_error :: IteratorPtr -> ErrPtr -> IO ()


--
-- Write batch
--

foreign import ccall safe "rocksdb\\c.h rocksdb_writebatch_create"
  c_rocksdb_writebatch_create :: IO WriteBatchPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_writebatch_destroy"
  c_rocksdb_writebatch_destroy :: WriteBatchPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_writebatch_clear"
  c_rocksdb_writebatch_clear :: WriteBatchPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_writebatch_put"
  c_rocksdb_writebatch_put :: WriteBatchPtr
                           -> Key -> CSize
                           -> Val -> CSize
                           -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_writebatch_delete"
  c_rocksdb_writebatch_delete :: WriteBatchPtr -> Key -> CSize -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_writebatch_iterate"
  c_rocksdb_writebatch_iterate :: WriteBatchPtr
                               -> Ptr ()                            -- ^ state
                               -> FunPtr (Ptr () -> Key -> CSize -> Val -> CSize) -- ^ put
                               -> FunPtr (Ptr () -> Key -> CSize)     -- ^ delete
                               -> IO ()

--
-- Options
--

foreign import ccall safe "rocksdb\\c.h rocksdb_options_create"
  c_rocksdb_options_create :: IO OptionsPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_options_destroy"
  c_rocksdb_options_destroy :: OptionsPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_create_if_missing"
  c_rocksdb_options_set_create_if_missing :: OptionsPtr -> CUChar -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_error_if_exists"
  c_rocksdb_options_set_error_if_exists :: OptionsPtr -> CUChar -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_paranoid_checks"
  c_rocksdb_options_set_paranoid_checks :: OptionsPtr -> CUChar -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_info_log"
  c_rocksdb_options_set_info_log :: OptionsPtr -> LoggerPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_write_buffer_size"
  c_rocksdb_options_set_write_buffer_size :: OptionsPtr -> CSize -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_max_open_files"
  c_rocksdb_options_set_max_open_files :: OptionsPtr -> CInt -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_options_set_compression"
  c_rocksdb_options_set_compression :: OptionsPtr -> CompressionOpt -> IO ()


type StatePtr   = Ptr ()
type Destructor = StatePtr -> ()
type NameFun    = StatePtr -> CString

-- | Make a destructor FunPtr
foreign import ccall "wrapper" mkDest :: Destructor -> IO (FunPtr Destructor)

-- | Make a name FunPtr
foreign import ccall "wrapper" mkName :: NameFun -> IO (FunPtr NameFun)

--
-- Read options
--

foreign import ccall safe "rocksdb\\c.h rocksdb_readoptions_create"
  c_rocksdb_readoptions_create :: IO ReadOptionsPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_readoptions_destroy"
  c_rocksdb_readoptions_destroy :: ReadOptionsPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_readoptions_set_verify_checksums"
  c_rocksdb_readoptions_set_verify_checksums :: ReadOptionsPtr -> CUChar -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_readoptions_set_fill_cache"
  c_rocksdb_readoptions_set_fill_cache :: ReadOptionsPtr -> CUChar -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_readoptions_set_snapshot"
  c_rocksdb_readoptions_set_snapshot :: ReadOptionsPtr -> SnapshotPtr -> IO ()

foreign import ccall unsafe "rocksdb\\c.h rocksdb_readoptions_set_iterate_lower_bound"
  c_rocksdb_readoptions_set_iterate_lower_bound :: ReadOptionsPtr -> Key -> CSize -> IO ()

foreign import ccall unsafe "rocksdb\\c.h rocksdb_readoptions_set_iterate_upper_bound"
  c_rocksdb_readoptions_set_iterate_upper_bound :: ReadOptionsPtr -> Key -> CSize -> IO ()

foreign import ccall unsafe "chainweb\\rocksdb.h rocksdb_readoptions_set_auto_prefix_mode"
  c_rocksdb_readoptions_set_auto_prefix_mode :: ReadOptionsPtr -> CBool -> IO ()

--
-- Write options
--

foreign import ccall safe "rocksdb\\c.h rocksdb_writeoptions_create"
  c_rocksdb_writeoptions_create :: IO WriteOptionsPtr

foreign import ccall safe "rocksdb\\c.h rocksdb_writeoptions_destroy"
  c_rocksdb_writeoptions_destroy :: WriteOptionsPtr -> IO ()

foreign import ccall safe "rocksdb\\c.h rocksdb_writeoptions_set_sync"
  c_rocksdb_writeoptions_set_sync :: WriteOptionsPtr -> CUChar -> IO ()


--
-- Cache
--

foreign import ccall safe "rocksdb\\c.h rocksdb_cache_create_lru"
  c_rocksdb_cache_create_lru :: CSize -> IO CachePtr

foreign import ccall safe "rocksdb\\c.h rocksdb_cache_destroy"
  c_rocksdb_cache_destroy :: CachePtr -> IO ()

----------------------------------------------------------------------------
-- Free
----------------------------------------------------------------------------

foreign import ccall safe "rocksdb\\c.h rocksdb_free"
  c_rocksdb_free :: CString -> IO ()
