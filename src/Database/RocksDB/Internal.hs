{-# LANGUAGE RecordWildCards #-}
-- |
-- Module      : Database.RocksDB.Internal
-- Copyright   : (c) 2012-2013 The leveldb-haskell Authors
--               (c) 2014 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : mail@agrafix.net
-- Stability   : experimental
-- Portability : non-portable
--

module Database.RocksDB.Internal
    ( -- * Types
      DB (..)
    , Options' (..)

    -- * "Smart" constructors and deconstructors
    , freeOpts
    , freeCString
    , mkOpts

    -- * combinators
    , withCWriteOpts
    , withCReadOpts

    -- * Utilities
    , throwIfErr
    , cSizeToInt
    , intToCSize
    , intToCInt
    , cIntToInt
    , boolToNum
    )
where

import           Control.Exception      (bracket, onException, throwIO)
import           Control.Monad          (when)
import           Data.ByteString        (ByteString)
import           Foreign
import           Foreign.C.String       (CString, peekCString, withCString)
import           Foreign.C.Types        (CInt, CSize)

import           Database.RocksDB.C
import           Database.RocksDB.Types

import qualified Data.ByteString        as BS
import qualified Data.ByteString.Unsafe       as BU


-- | Database handle
data DB = DB !RocksDBPtr !Options'

instance Eq DB where
    (DB pt1 _) == (DB pt2 _) = pt1 == pt2

-- | Internal representation of the 'Options'
data Options' = Options'
    { _optsPtr  :: !OptionsPtr
    , _cachePtr :: !(Maybe CachePtr)
    }

mkOpts :: Options -> IO Options'
mkOpts Options{..} = do
    opts_ptr <- c_rocksdb_options_create

    c_rocksdb_options_set_compression opts_ptr
        $ ccompression compression
    c_rocksdb_options_set_create_if_missing opts_ptr
        $ boolToNum createIfMissing
    c_rocksdb_options_set_error_if_exists opts_ptr
        $ boolToNum errorIfExists
    c_rocksdb_options_set_max_open_files opts_ptr
        $ intToCInt maxOpenFiles
    c_rocksdb_options_set_paranoid_checks opts_ptr
        $ boolToNum paranoidChecks
    c_rocksdb_options_set_write_buffer_size opts_ptr
        $ intToCSize writeBufferSize

    return (Options' opts_ptr Nothing)

  where
    ccompression NoCompression =
        noCompression
    ccompression SnappyCompression =
        snappyCompression
    ccompression ZlibCompression =
        zlibCompression

freeOpts :: Options' -> IO ()
freeOpts (Options' opts_ptr mcache_ptr ) = do
    c_rocksdb_options_destroy opts_ptr
    maybe (return ()) c_rocksdb_cache_destroy mcache_ptr

withCReadOpts :: (ReadOptionsPtr -> IO a) -> IO a
withCReadOpts =
    bracket c_rocksdb_readoptions_create c_rocksdb_readoptions_destroy

withCWriteOpts :: (WriteOptionsPtr -> IO a) -> IO a
withCWriteOpts =
    bracket c_rocksdb_writeoptions_create c_rocksdb_writeoptions_destroy

freeCString :: CString -> IO ()
freeCString = c_rocksdb_free

throwIfErr :: String -> (ErrPtr -> IO a) -> IO a
throwIfErr s f = alloca $ \err_ptr -> do
    poke err_ptr nullPtr
    res  <- f err_ptr
    erra <- peek err_ptr
    when (erra /= nullPtr) $ do
        err <- peekCString erra
        throwIO $ userError $ s ++ ": " ++ err
    return res

cSizeToInt :: CSize -> Int
cSizeToInt = fromIntegral
{-# INLINE cSizeToInt #-}

intToCSize :: Int -> CSize
intToCSize = fromIntegral
{-# INLINE intToCSize #-}

intToCInt :: Int -> CInt
intToCInt = fromIntegral
{-# INLINE intToCInt #-}

cIntToInt :: CInt -> Int
cIntToInt = fromIntegral
{-# INLINE cIntToInt #-}

boolToNum :: Num b => Bool -> b
boolToNum True  = fromIntegral (1 :: Int)
boolToNum False = fromIntegral (0 :: Int)
{-# INLINE boolToNum #-}
