{-# language RankNTypes #-}

-- |
-- Module      : Database.RocksDB.Types
-- Copyright   : (c) 2012-2013 The leveldb-haskell Authors
--               (c) 2014 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : mail@agrafix.net
-- Stability   : experimental
-- Portability : non-portable
--

module Database.RocksDB.Types
    ( BatchOp (..)
    , BloomFilter (..)
    , Compression (..)
    , Options (..)
    , Property (..)
    , ReadOptions (..)
    , WriteBatch
    , WriteOptions (..)

    , defaultOptions
    )
where

import           Data.ByteString    (ByteString)
import           Foreign

import           Database.RocksDB.C

-- | Compression setting
data Compression
    = NoCompression
    | SnappyCompression
    | ZlibCompression
    deriving (Eq, Show)

-- | Represents the built-in Bloom Filter
newtype BloomFilter = BloomFilter FilterPolicyPtr

-- | Options when opening a database
data Options = Options
    { compression     :: !Compression
      -- ^ Compress blocks using the specified compression algorithm.
      --
      -- This parameter can be changed dynamically.
      --
      -- Default: 'EnableCompression'
    , createIfMissing :: !Bool
      -- ^ If true, the database will be created if it is missing.
      --
      -- Default: False
    , errorIfExists   :: !Bool
      -- ^ It true, an error is raised if the database already exists.
      --
      -- Default: False
    , maxOpenFiles    :: !Int
      -- ^ Number of open files that can be used by the DB.
      --
      -- You may need to increase this if your database has a large working set
      -- (budget one open file per 2MB of working set).
      --
      -- Default: 1000
    , paranoidChecks  :: !Bool
      -- ^ If true, the implementation will do aggressive checking of the data
      -- it is processing and will stop early if it detects any errors.
      --
      -- This may have unforeseen ramifications: for example, a corruption of
      -- one DB entry may cause a large number of entries to become unreadable
      -- or for the entire DB to become unopenable.
      --
      -- Default: False
    , writeBufferSize :: !Int
      -- ^ Amount of data to build up in memory (backed by an unsorted log on
      -- disk) before converting to a sorted on-disk file.
      --
      -- Larger values increase performance, especially during bulk loads. Up to
      -- to write buffers may be held in memory at the same time, so you may
      -- with to adjust this parameter to control memory usage. Also, a larger
      -- write buffer will result in a longer recovery time the next time the
      -- database is opened.
      --
      -- Default: 4MB
    }

defaultOptions :: Options
defaultOptions = Options
    { compression          = SnappyCompression
    , createIfMissing      = False
    , errorIfExists        = False
    , maxOpenFiles         = 1000
    , paranoidChecks       = False
    , writeBufferSize      = 4 `shift` 20
    }

-- | Options for write operations
-- data WriteOptions = WriteOptions
    -- { sync :: !Bool
      -- ^ If true, the write will be flushed from the operating system buffer
      -- cache (by calling WritableFile::Sync()) before the write is considered
      -- complete. If this flag is true, writes will be slower.
      --
      -- If this flag is false, and the machine crashes, some recent writes may
      -- be lost. Note that if it is just the process that crashes (i.e., the
      -- machine does not reboot), no writes will be lost even if sync==false.
      --
      -- In other words, a DB write with sync==false has similar crash semantics
      -- as the "write()" system call. A DB write with sync==true has similar
      -- crash semantics to a "write()" system call followed by "fsync()".
      --
      -- Default: False
    -- } deriving (Eq, Show)

newtype WriteOptions = WriteOptions
    { runWriteOptions :: forall r. WriteOptionsPtr -> IO r -> IO r
    }

instance Monoid WriteOptions where
    mempty = WriteOptions (\_ k -> k)

instance Semigroup WriteOptions where
    WriteOptions r <> WriteOptions r' =
        WriteOptions $ \ptr k -> r ptr (r' ptr k)

newtype ReadOptions = ReadOptions
    { runReadOptions :: forall r. ReadOptionsPtr -> IO r -> IO r
    }

instance Monoid ReadOptions where
    mempty = ReadOptions (\_ k -> k)

instance Semigroup ReadOptions where
    ReadOptions r <> ReadOptions r' =
        ReadOptions $ \ptr k -> r ptr (r' ptr k)

type WriteBatch = [BatchOp]

-- | Batch operation
data BatchOp = Put ByteString ByteString | Del ByteString
    deriving (Eq, Show)

-- | Properties exposed by RocksDB
data Property = NumFilesAtLevel Int | Stats | SSTables
    deriving (Eq, Show)
