{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- |
-- Module: Data.CAS.RocksDB
-- Copyright: Copyright © 2019 Kadena LLC.
-- License: MIT
-- Maintainer: Lars Kuhtz <lars@kadena.io>
-- Stability: experimental
--
-- Persisted iey-value and and content-addressable-key-value stores with a
-- RocksDB backend.
--
-- A 'RocksDbTable' provides a typed key-value store with an additional iterator
-- interace.
--
-- 'RocksDbCas' adds an 'IsCas' instance for a 'RocksDbTable' where the value
-- type is content-addressable.
--
-- TODO: Abstract the 'RocksDbTable' API into a typeclass so that one can
-- provide alterantive implementations for it.

-- This module contains some code modified from github.com/serokell/rocksdb-haskell, 
-- copyright the rocksdb-haskell and leveldb-haskell authors.
--
module Data.CAS.RocksDB
( RocksDb(..)
, rocksDbHandle
, rocksDbNamespace
, openRocksDb
, closeRocksDb
, withRocksDb
, withTempRocksDb
, destroyRocksDb
, resetOpenRocksDb

-- * Rocks DB Table
, Codec(..)
, RocksDbTable
, newTable
, tableLookup
, tableInsert
, tableDelete

-- * Batch Updates
, RocksDbUpdate(..)
, updateBatch

-- * Rocks DB Table Iterator
, RocksDbTableIter
, createTableIter
, releaseTableIter
, withTableIter
, tableIterValid

-- ** Seeking
, tableIterSeek
, tableIterFirst
, tableIterLast

-- ** Advance Iterator
, tableIterNext
, tableIterPrev

-- ** Query Iterator
, tableIterEntry
, tableIterValue
, tableIterKey

-- ** Streams
, iterToEntryStream
, iterToValueStream
, iterToKeyStream

-- ** Extremal Table Entries
, tableMaxKey
, tableMaxValue
, tableMaxEntry
, tableMinKey
, tableMinValue
, tableMinEntry

-- * RocksDbCas
, RocksDbCas(..)
, newCas

-- * RocksDB-specific tools
, checkpointRocksDb
, approxTableSizeRocksDb
, deleteRange
, R.defaultOptions
, R.Options(..)
) where

import Control.Lens
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8

import Data.CAS
import Data.String
import qualified Data.Text as T
import qualified Data.Vector as V

import Foreign.C
import Foreign.Marshal
import Foreign.Ptr
import Foreign.Storable

import qualified Database.RocksDB.Base as R
import qualified Database.RocksDB.C as C
import qualified Database.RocksDB.Internal as R
import qualified Database.RocksDB.Iterator as I

import GHC.Generics (Generic)
import qualified GHC.Foreign as GHC
import qualified GHC.IO.Encoding as GHC
import GHC.Stack

import NoThunks.Class

import qualified Streaming.Prelude as S

import System.Directory
import System.IO.Temp

-- -------------------------------------------------------------------------- --
-- Utils

-- | A newtype wrapper for tagger values as "expected" outcomes of some
-- computation.
--
newtype Expected a = Expected { getExpected :: a }
    deriving (Show, Eq, Ord, Generic, Functor)
    deriving newtype (NoThunks)

-- | A newtype wrapper for tagger values as "actual" outcomes of some
-- computation.
--
newtype Actual a = Actual { getActual :: a }
    deriving (Show, Eq, Ord, Generic, Functor)
    deriving newtype (NoThunks)

-- | A textual message that describes the 'Expected' and the 'Actual' outcome of
-- some computation.
--
unexpectedMsg :: Show a => T.Text -> Expected a -> Actual a -> T.Text
unexpectedMsg msg expected actual = msg
    <> ", expected: " <> sshow (getExpected expected)
    <> ", actual: " <> sshow (getActual actual)

-- | Show a value as any type that is an instance of 'IsString'.
--
sshow :: Show a => IsString b => a -> b
sshow = fromString . show
{-# INLINE sshow #-}

-- -------------------------------------------------------------------------- --
-- RocksDb

data ColumnFamilyHandle

data RocksDbHandle = RocksDbHandle
    { _rocksDbHandleUnderlying :: !R.DB
    , _rocksDbHandleDefaultCf :: !(Ptr ColumnFamilyHandle)
    }

-- | This wrapper allows to create key namespaces. This is, for instance, useful
-- for testing when running several concurrent tests on the same rocks db
-- instances. Key namespaces must not contain the character '-'.
--
data RocksDb = RocksDb
    { _rocksDbHandle :: !RocksDbHandle
    , _rocksDbNamespace :: !B.ByteString
    }

instance NoThunks RocksDb where
    wNoThunks ctx (RocksDb a b) = allNoThunks
        [ noThunks ctx (InspectHeapNamed @"Data.RocksDB.Base.DB" a)
        , noThunks ctx b
        ]
    showTypeOf _ = "Data.CAS.RocksDB.RocksDb"
    {-# INLINE wNoThunks #-}
    {-# INLINE showTypeOf #-}

makeLenses ''RocksDb

 
foreign import ccall unsafe "rocksdb\\c.h rocksdb_open_column_families"
    rocksdb_open_column_families
        :: C.OptionsPtr {- database options -}
        -> CString {- database name -}
        -> CInt {- number of column families -}
        -> Ptr CString {- column family names -}
        -> Ptr C.OptionsPtr {- array of column family options -}
        -> Ptr (Ptr ColumnFamilyHandle) {- output: array of pointers for holding column family handles -}
        -> Ptr CString {- output: errptr -}
        -> IO C.RocksDBPtr

-- | Open a 'RocksDb' instance with the default namespace. If no rocks db exists
-- at the provided directory path, a new database is created.
--
-- This function is copied mostly unmodified from rocksdb-haskell *except* that we need
-- a handle on the default column family.
openRocksDb :: FilePath -> R.Options -> IO RocksDb
openRocksDb path opts = bracketOnError initialize finalize mkDB
  where
#ifdef mingw32_HOST_OS
    initialize =
        (, ()) <$> R.mkOpts opts
    finalize (opts', ()) =
        R.freeOpts opts'
#else
    initialize = do
        opts' <- R.mkOpts opts
        -- With LC_ALL=C, two things happen:
        --   * rocksdb can't open a database with unicode in path;
        --   * rocksdb can't create a folder properly.
        -- So, we create the folder by ourselves, and for thart we
        -- need to set the encoding we're going to use. On Linux
        -- it's almost always UTC-8.
        oldenc <- GHC.getFileSystemEncoding
        when (R.createIfMissing opts) $
            GHC.setFileSystemEncoding GHC.utf8
        pure (opts', oldenc)
    finalize (opts', oldenc) = do
        R.freeOpts opts'
        GHC.setFileSystemEncoding oldenc
#endif
    mkDB (opts'@(R.Options' opts_ptr _ _), _) = do
        when (R.createIfMissing opts) $
            createDirectoryIfMissing True path
        db <- alloca $ \colFamOptions ->
            alloca $ \colFamNames ->
            alloca $ \colFamHandles ->
            withCString "default" $ \colFamName ->
            withFilePath path $ \path_ptr -> do
                poke colFamNames colFamName
                poke colFamOptions opts_ptr
                dbUnderlying <- fmap (`R.DB` opts')
                    $ checked "Data.CAS.RocksDB.openRocksDb"
                    $ rocksdb_open_column_families opts_ptr path_ptr 1 colFamNames colFamOptions colFamHandles
                colFamHandle <- peek colFamHandles
                return $ RocksDb (RocksDbHandle dbUnderlying colFamHandle) mempty
        initializeRocksDb db
        return db

-- | Each table key starts with @_rocksDbNamespace db <> "-"@. Here we insert a
-- dummy key that is guaranteed to be appear after any other key in the
-- database. We rely on its existence that in the implementation of
-- 'tableIteratorLast'.
--
initializeRocksDb :: RocksDb -> IO ()
initializeRocksDb db = R.put
    (_rocksDbHandleUnderlying $ _rocksDbHandle db)
    R.defaultWriteOptions
    (_rocksDbNamespace db <> ".")
    ""

-- | Open a 'RocksDb' and reset it if it already exists.
--
resetOpenRocksDb :: FilePath -> IO RocksDb
resetOpenRocksDb path = do
    destroyRocksDb path
    openRocksDb path R.defaultOptions { R.createIfMissing = True, R.errorIfExists = True }

-- | Close a 'RocksDb' instance.
--
closeRocksDb :: RocksDb -> IO ()
closeRocksDb = R.close . _rocksDbHandleUnderlying . _rocksDbHandle

-- | Provide a computation with a 'RocksDb' instance. If no rocks db exists at
-- the provided directory path, a new database is created.
--
withRocksDb :: FilePath -> R.Options -> (RocksDb -> IO a) -> IO a
withRocksDb path opts = bracket (openRocksDb path opts) closeRocksDb

-- | Provide a computation with a temporary 'RocksDb'. The database is deleted
-- when the computation exits.
--
withTempRocksDb :: String -> (RocksDb -> IO a) -> IO a
withTempRocksDb template f = withSystemTempDirectory template $ \dir ->
    withRocksDb dir R.defaultOptions { R.createIfMissing = True } f

-- | Delete the RocksDb instance.
--
-- This is the prefered method of deleting an rocks db instance. A rocks db
-- instance may store files in different locations. This function guarantees
-- that all files are deleted.
--
destroyRocksDb :: FilePath -> IO ()
destroyRocksDb path = R.destroy path opts
  where
    opts = R.defaultOptions { R.createIfMissing = False }

-- -------------------------------------------------------------------------- --
-- RocksDb Table

-- NOTE: the implementation currently doesnt' support iteration over nested
-- namespaces. This could easily be added via an iterator that iterates over
-- all respective prefixes ending with '/' or '$' and using a codec that
-- could handle the types in all sub-namespaces. This could for instance
-- be useful for iterating over the complete database.

-- | A binary codec for encoding and decoding values that are stored in a
-- 'RocksDb' Table.
--
data Codec a = Codec
    { _codecEncode :: !(a -> B.ByteString)
        -- ^ encode a value.
    , _codecDecode :: !(forall m . MonadThrow m => B.ByteString -> m a)
        -- ^ decode a value. Throws an exception of decoding fails.
    }

instance NoThunks (Codec a) where
    wNoThunks c (Codec a _) = allNoThunks
        [ noThunks c a
          -- _codecDecode is existentially quantified and the instance dictionary
          -- reference is a thunk, even thought the field is strict
        ]
    showTypeOf _ = "Data.CAS.RocksDB.Codec"
    {-# INLINE wNoThunks #-}
    {-# INLINE showTypeOf #-}

-- | A logical table in a 'RocksDb'. Tables in a rocks db are have isolated key
-- namespaces.
--
data RocksDbTable k v = RocksDbTable
    { _rocksDbTableValueCodec :: !(Codec v)
    , _rocksDbTableKeyCodec :: !(Codec k)
    , _rocksDbTableNamespace :: !B.ByteString
    , _rocksDbTableDb :: !RocksDbHandle
    }

instance NoThunks (RocksDbTable k v) where
    wNoThunks ctx (RocksDbTable a b c d) = allNoThunks
        [ noThunks ctx a
        , noThunks ctx b
        , noThunks ctx c
        , noThunks ctx (InspectHeapNamed @"Data.RocksDB.Base.DB" d)
        ]
    showTypeOf _ = "Data.CAS.RocksDB.RocksDbTable"
    {-# INLINE wNoThunks #-}
    {-# INLINE showTypeOf #-}

-- | Create a new 'RocksDbTable' in the given 'RocksDb'.
--
-- Table name components must NOT contain any of '$', '%', and '/'. A user
-- error is raised if the namespace contains any of these characters.
--
newTable
    :: HasCallStack
    => RocksDb
    -> Codec v
    -> Codec k
    -> [B.ByteString]
    -> RocksDbTable k v
newTable db valCodec keyCodec namespace
    | any (B8.any (\x -> x `elem` ['$', '%', '/'])) namespace
        = error $ "Data.CAS.RocksDb.newTable: invalid character in table namespace: " <> sshow namespace
    | otherwise
        = RocksDbTable valCodec keyCodec ns (_rocksDbHandle db)
  where
    ns = _rocksDbNamespace db <> "-" <> B.intercalate "/" namespace
{-# INLINE newTable #-}

-- | @tableInsert db k v@ inserts the value @v@ at key @k@ in the rocks db table
-- @db@.
--
tableInsert :: RocksDbTable k v -> k -> v -> IO ()
tableInsert db k v = R.put
    (_rocksDbHandleUnderlying (_rocksDbTableDb db))
    R.defaultWriteOptions
    (encKey db k)
    (encVal db v)
{-# INLINE tableInsert #-}

-- | @tableLookup db k@ returns 'Just' the value at key @k@ in the
-- 'RocksDbTable' @db@ if it exists, or 'Nothing' if the @k@ doesn't exist in
-- the table.
--
tableLookup :: RocksDbTable k v -> k -> IO (Maybe v)
tableLookup db k = do
    maybeBytes <- R.get (_rocksDbHandleUnderlying (_rocksDbTableDb db)) R.defaultReadOptions (encKey db k)
    traverse (decVal db) maybeBytes
{-# INLINE tableLookup #-}

-- | @tableDelete db k@ deletes the value at the key @k@ from the 'RocksDbTable'
-- db. If the @k@ doesn't exist in @db@ this function does nothing.
--
tableDelete :: RocksDbTable k v -> k -> IO ()
tableDelete db k = R.delete
    (_rocksDbHandleUnderlying (_rocksDbTableDb db))
    R.defaultWriteOptions
    (encKey db k)
{-# INLINE tableDelete #-}

-- -------------------------------------------------------------------------- --
-- Batches

-- | A single update operation for a 'RocksDbTable'
--
data RocksDbUpdate
    = forall k v . RocksDbDelete
        { _rocksDbUpdateTable :: !(RocksDbTable k v)
        , _rocksDbUpdateKey :: !k
        }
    | forall k v . RocksDbInsert
        { _rocksDbUpdateTable :: !(RocksDbTable k v)
        , _rocksDbUpdateKey :: !k
        , _rocksDbUpdateValue :: !v
        }

rocksDbUpdateDb :: RocksDbUpdate -> RocksDbHandle
rocksDbUpdateDb (RocksDbDelete t _) = _rocksDbTableDb t
rocksDbUpdateDb (RocksDbInsert t _ _) = _rocksDbTableDb t
{-# INLINE rocksDbUpdateDb #-}

-- | Atomically execute a batch of rocks db updates.
--
-- All tables in the batch operations belong to the same rocks db instance.
-- Otherwise an error is raised.
--
updateBatch :: HasCallStack => [RocksDbUpdate] -> IO ()
updateBatch [] = return ()
updateBatch batch = R.write rdb R.defaultWriteOptions $ checkMkOp <$> batch
  where
    rdb = _rocksDbHandleUnderlying $ rocksDbUpdateDb $ head batch

    checkMkOp o
        | rdb == _rocksDbHandleUnderlying (rocksDbUpdateDb o) = mkOp o
        | otherwise = error "Data.CAS.RocksDB.updateBatch: all operations in a batch must be for the same RocksDB instance."

    mkOp (RocksDbDelete t k) = R.Del (encKey t k)
    mkOp (RocksDbInsert t k v) = R.Put (encKey t k) (encVal t v)

-- -------------------------------------------------------------------------- --
-- Table Iterator

-- | An iterator for a 'RocksDbTable'. An interator is a stateful object that
-- represents an enumeration of a subset of the entries of an immutable snapshop
-- of the table.
--
-- An iterator should be used single threaded. Since the iterator retains the
-- snapshot of the database, one should avoid storing iterators over longer
-- periods of time. After usage it should be released in a timely manner.
--
-- The recommended way to created a 'RocksDbTableIter' is to use the function
-- `withTableIter`.
--
data RocksDbTableIter k v = RocksDbTableIter
    { _rocksDbTableIterValueCodec :: !(Codec v)
    , _rocksDbTableIterKeyCodec :: !(Codec k)
    , _rocksDbTableIterNamespace :: !B.ByteString
    , _rocksDbTableIter :: !I.Iterator
    }

instance NoThunks (RocksDbTableIter k v) where
    wNoThunks ctx (RocksDbTableIter a b c d) = allNoThunks
        [ noThunks ctx a
        , noThunks ctx b
        , noThunks ctx c
        , noThunks ctx (InspectHeapNamed @"Data.RocksDB.Iterator.Iterator" d)
        ]
    showTypeOf _ = "Data.CAS.RocksDB.RocksDbTableIterator"
    {-# INLINE wNoThunks #-}
    {-# INLINE showTypeOf #-}

-- | Creates a 'RocksDbTableIterator'. If the 'RocksDbTable' is not empty, the
-- iterator is pointing to the first key in the 'RocksDbTable' and is valid.
--
-- The returnd iterator must be released with 'releaseTableIter' when it is not
-- needed any more. Not doing so release in a data leak that retains database
-- snapshots.
--
createTableIter :: RocksDbTable k v -> IO (RocksDbTableIter k v)
createTableIter db = do
    !tit <- RocksDbTableIter
        (_rocksDbTableValueCodec db)
        (_rocksDbTableKeyCodec db)
        (_rocksDbTableNamespace db)
        <$> I.createIter (_rocksDbHandleUnderlying $ _rocksDbTableDb db) R.defaultReadOptions
    tableIterFirst tit
    return tit
{-# INLINE createTableIter #-}

-- | Releases an 'RocksDbTableIteror', freeing up it's resources.
--
releaseTableIter :: RocksDbTableIter k v -> IO ()
releaseTableIter = I.releaseIter . _rocksDbTableIter
{-# INLINE releaseTableIter #-}

-- | Provide an computation with a 'RocksDbTableIteror' and release the iterator
-- after after the computation has finished either by returning a result or
-- throwing an exception.
--
-- This is function provides the prefered way of creating and using a
-- 'RocksDbTableIter'.
--
withTableIter :: RocksDbTable k v -> (RocksDbTableIter k v -> IO a) -> IO a
withTableIter db = bracket (createTableIter db) releaseTableIter
{-# INLINE withTableIter #-}

-- | Checks if an 'RocksDbTableIterator' is valid.
--
-- A valid iterator returns a value when 'tableIterEntry', 'tableIterValue', or
-- 'tableIterKey' is called on it.
--
tableIterValid :: MonadIO m => RocksDbTableIter k v -> m Bool
tableIterValid it = I.iterKey (_rocksDbTableIter it) >>= \case
    Nothing -> return False
    (Just !x) -> return $! checkIterKey it x
{-# INLINE tableIterValid #-}

-- | Efficiently seek to a key in a 'RocksDbTableIterator' iteration.
--
tableIterSeek :: MonadIO m => RocksDbTableIter k v -> k -> m ()
tableIterSeek it = I.iterSeek (_rocksDbTableIter it) . encIterKey it
{-# INLINE tableIterSeek #-}

-- | Seek to the first key in a 'RocksDbTable'.
--
tableIterFirst :: MonadIO m => RocksDbTableIter k v -> m ()
tableIterFirst it
    = I.iterSeek (_rocksDbTableIter it) $ namespaceFirst (_rocksDbTableIterNamespace it)
{-# INLINE tableIterFirst #-}

-- | Seek to the last value in a 'RocksDbTable'
--
tableIterLast :: MonadIO m => RocksDbTableIter k v -> m ()
tableIterLast it = do
    I.iterSeek (_rocksDbTableIter it) $ namespaceLast (_rocksDbTableIterNamespace it)
    I.iterPrev (_rocksDbTableIter it)
{-# INLINE tableIterLast #-}

-- | Move a 'RocksDbTableIter' to the next key in a 'RocksDbTable'.
--
tableIterNext :: MonadIO m => RocksDbTableIter k v -> m ()
tableIterNext = I.iterNext . _rocksDbTableIter
{-# INLINE tableIterNext #-}

-- | Move a 'RocksDbTableIter' to the previous key in a 'RocksDbTable'.
--
tableIterPrev :: MonadIO m => RocksDbTableIter k v -> m ()
tableIterPrev = I.iterPrev . _rocksDbTableIter
{-# INLINE tableIterPrev #-}

-- | Returns the key and the value at the current position of a
-- 'RocksDbTableIter'. Returns 'Nothing' if the iterator is invalid.
--
tableIterEntry
    :: MonadIO m
    => MonadThrow m
    => RocksDbTableIter k v
    -> m (Maybe (k, v))
tableIterEntry it = I.iterEntry (_rocksDbTableIter it) >>= \case
    Nothing -> return Nothing
    Just (k, v) -> do
        tryDecIterKey it k >>= \case
            Nothing -> return Nothing
            (Just !k') -> do
                !v' <- decIterVal it v
                return $! Just $! (k', v')
{-# INLINE tableIterEntry #-}

-- | Returns the value at the current position of a 'RocksDbTableIter'. Returns
-- 'Nothing' if the iterator is invalid.
--
tableIterValue
    :: MonadIO m
    => MonadThrow m
    => RocksDbTableIter k v
    -> m (Maybe v)
tableIterValue it = fmap snd <$> tableIterEntry it
{-# INLINE tableIterValue #-}

-- | Returns the key at the current position of a 'RocksDbTableIter'. Returns
-- 'Nothing' if the iterator is invalid.
--
tableIterKey
    :: MonadIO m
    => MonadThrow m
    => RocksDbTableIter k v
    -> m (Maybe k)
tableIterKey it = I.iterKey (_rocksDbTableIter it) >>= \case
    Nothing -> return Nothing
    Just k -> tryDecIterKey it k
{-# INLINE tableIterKey #-}

-- | Returns the stream of key-value pairs of an 'RocksDbTableIter'.
--
-- The iterator must be released after the stream is consumed. Releasing the
-- iterator to early while the stream is still in use results in a runtime
-- error. Not releasing the iterator after the processing of the stream has
-- finished results in a memory leak.
--
iterToEntryStream :: MonadIO m => RocksDbTableIter k v -> S.Stream (S.Of (k,v)) m ()
iterToEntryStream it = liftIO (tableIterEntry it) >>= \case
    Nothing -> return ()
    Just x -> S.yield x >> tableIterNext it >> iterToEntryStream it
{-# INLINE iterToEntryStream #-}

-- | Returns the stream of values of an 'RocksDbTableIter'.
--
-- The iterator must be released after the stream is consumed. Releasing the
-- iterator to early while the stream is still in use results in a runtime
-- error. Not releasing the iterator after the processing of the stream has
-- finished results in a memory leak.
--
iterToValueStream :: MonadIO m => RocksDbTableIter k v -> S.Stream (S.Of v) m ()
iterToValueStream it = liftIO (tableIterValue it) >>= \case
    Nothing -> return ()
    Just x -> S.yield x >> tableIterNext it >> iterToValueStream it
{-# INLINE iterToValueStream #-}

-- | Returns the stream of keys of an 'RocksDbTableIter'.
--
-- The iterator must be released after the stream is consumed. Releasing the
-- iterator to early while the stream is still in use results in a runtime
-- error. Not releasing the iterator after the processing of the stream has
-- finished results in a memory leak.
--
iterToKeyStream :: MonadIO m => RocksDbTableIter k v -> S.Stream (S.Of k) m ()
iterToKeyStream it = liftIO (tableIterKey it) >>= \case
    Nothing -> return ()
    Just x -> S.yield x >> tableIterNext it >> iterToKeyStream it
{-# INLINE iterToKeyStream #-}

-- Extremal Table Entries

-- | The maximum key in a 'RocksDbTable'.
--
tableMaxKey :: RocksDbTable k v -> IO (Maybe k)
tableMaxKey = flip withTableIter $ \i -> tableIterLast i *> tableIterKey i
{-# INLINE tableMaxKey #-}

-- | The maximum value in a 'RocksDbTable'.
--
tableMaxValue :: RocksDbTable k v -> IO (Maybe v)
tableMaxValue = flip withTableIter $ \i -> tableIterLast i *> tableIterValue i
{-# INLINE tableMaxValue #-}

-- | The maximum key-value in a 'RocksDbTable'.
--
tableMaxEntry :: RocksDbTable k v -> IO (Maybe (k, v))
tableMaxEntry = flip withTableIter $ \i -> tableIterLast i *> tableIterEntry i
{-# INLINE tableMaxEntry #-}

-- | The minimum key in a 'RocksDbTable'.
--
tableMinKey :: RocksDbTable k v -> IO (Maybe k)
tableMinKey = flip withTableIter $ \i -> tableIterFirst i *> tableIterKey i
{-# INLINE tableMinKey #-}

-- | The minimum value in a 'RocksDbTable'.
--
tableMinValue :: RocksDbTable k v -> IO (Maybe v)
tableMinValue = flip withTableIter $ \i -> tableIterFirst i *> tableIterValue i
{-# INLINE tableMinValue #-}

-- | The minimum key-value in a 'RocksDbTable'.
--
tableMinEntry :: RocksDbTable k v -> IO (Maybe (k, v))
tableMinEntry = flip withTableIter $ \i -> tableIterFirst i *> tableIterEntry i
{-# INLINE tableMinEntry #-}

-- -------------------------------------------------------------------------- --
-- CAS

-- | For a 'IsCasValue' @v@ with 'CasKeyType v ~ k@,  a 'RocksDbTable k v' is an
-- instance of 'HasCasLookup'.
--
instance (IsCasValue v, CasKeyType v ~ k) => HasCasLookup (RocksDbTable k v) where
    type CasValueType (RocksDbTable k v) = v
    casLookup = tableLookup
    {-# INLINE casLookup #-}

-- | For a 'IsCasValue' @v@ with 'CasKeyType v ~ k@,  a 'RocksDbTable k v' is an
-- instance of 'IsCas'.
--
instance (IsCasValue v, CasKeyType v ~ k) => IsCas (RocksDbTable k v) where
    casInsert db a = tableInsert db (casKey a) a
    casDelete = tableDelete

    casInsertBatch db vs = updateBatch (mkOp <$> V.toList vs)
      where
        mkOp v = RocksDbInsert db (casKey v) v

    casDeleteBatch db vs = updateBatch (RocksDbDelete db <$> V.toList vs)

    {-# INLINE casInsert #-}
    {-# INLINE casDelete #-}
    {-# INLINE casInsertBatch #-}
    {-# INLINE casDeleteBatch #-}

-- | A newtype wrapper that takes only a single type constructor. This useful in
-- situations where a Higher Order type constructor for a CAS is required. A
-- type synonym doesn't work in this situation because type synonyms must be
-- fully applied.
--
newtype RocksDbCas v = RocksDbCas { _getRocksDbCas :: RocksDbTable (CasKeyType v) v }
    deriving newtype (NoThunks)

instance IsCasValue v => HasCasLookup (RocksDbCas v) where
    type CasValueType (RocksDbCas v) = v
    casLookup (RocksDbCas x) = casLookup x
    {-# INLINE casLookup #-}

instance IsCasValue v => IsCas (RocksDbCas v) where
    casInsert (RocksDbCas x) = casInsert x
    casDelete (RocksDbCas x) = casDelete x
    casInsertBatch (RocksDbCas x) = casInsertBatch x
    casDeleteBatch (RocksDbCas x) = casDeleteBatch x

    {-# INLINE casInsert #-}
    {-# INLINE casDelete #-}
    {-# INLINE casInsertBatch #-}
    {-# INLINE casDeleteBatch #-}

-- | Create a new 'RocksDbCas'.
--
newCas
    :: CasKeyType v ~ k
    => RocksDb
    -> Codec v
    -> Codec k
    -> [B.ByteString]
    -> RocksDbCas v
newCas db vc kc n = RocksDbCas $ newTable db vc kc n
{-# INLINE newCas #-}

-- -------------------------------------------------------------------------- --
-- Exceptions

-- | Excpeptions that can be thrown by functions in this module.
--
data RocksDbException
    = RocksDbTableIterInvalidKeyNamespace (Expected B.ByteString) (Actual B.ByteString)
    deriving (Eq, Ord, Generic, NoThunks)

instance Exception RocksDbException where
    displayException (RocksDbTableIterInvalidKeyNamespace e a)
        = T.unpack $ unexpectedMsg "Data.CAS.RocksDB: invalid table key" e a
    {-# INLINE displayException #-}

instance Show RocksDbException where
    show = displayException
    {-# INLINE show #-}

-- -------------------------------------------------------------------------- --
-- Table Utils

encVal :: RocksDbTable k v -> v -> B.ByteString
encVal = _codecEncode . _rocksDbTableValueCodec
{-# INLINE encVal #-}

encKey :: RocksDbTable k v -> k -> B.ByteString
encKey it k = namespaceFirst ns <> _codecEncode (_rocksDbTableKeyCodec it) k
  where
    ns = _rocksDbTableNamespace it
{-# INLINE encKey #-}

decVal :: MonadThrow m => RocksDbTable k v -> B.ByteString -> m v
decVal tbl = _codecDecode $ _rocksDbTableValueCodec tbl
{-# INLINE decVal #-}

-- -------------------------------------------------------------------------- --
-- Iter Utils

namespaceFirst :: B.ByteString -> B.ByteString
namespaceFirst ns = ns <> "$"
{-# INLINE namespaceFirst #-}

namespaceLast :: B.ByteString -> B.ByteString
namespaceLast ns = ns <> "%"
{-# INLINE namespaceLast #-}

encIterKey :: RocksDbTableIter k v -> k -> B.ByteString
encIterKey it k = namespaceFirst ns <> _codecEncode (_rocksDbTableIterKeyCodec it) k
  where
    ns = _rocksDbTableIterNamespace it
{-# INLINE encIterKey #-}

decIterVal :: MonadThrow m => RocksDbTableIter k v -> B.ByteString -> m v
decIterVal i = _codecDecode $ _rocksDbTableIterValueCodec i
{-# INLINE decIterVal #-}

checkIterKey :: RocksDbTableIter k v -> B.ByteString -> Bool
checkIterKey it k = maybe False (const True) $ decIterKey it k
{-# INLINE checkIterKey #-}

-- | Return 'Nothing' if the namespace doesn't match, and throws if the
-- key can't be decoded.
--
-- This function is useful because invalid iterators are represented as
-- iterators that point outside their respective namespace key range.
--
tryDecIterKey :: MonadThrow m => RocksDbTableIter k v -> B.ByteString -> m (Maybe k)
tryDecIterKey it k = case B.splitAt (B.length prefix) k of
    (a, b)
        | a /= prefix -> return Nothing
        | otherwise -> Just <$> _codecDecode (_rocksDbTableIterKeyCodec it) b
  where
    prefix = namespaceFirst $ _rocksDbTableIterNamespace it
{-# INLINE tryDecIterKey #-}

decIterKey :: MonadThrow m => RocksDbTableIter k v -> B.ByteString -> m k
decIterKey it k = case B.splitAt (B.length prefix) k of
    (a, b)
        | a == prefix -> _codecDecode (_rocksDbTableIterKeyCodec it) b
        | otherwise -> throwM
            $ RocksDbTableIterInvalidKeyNamespace (Expected $ _rocksDbTableIterNamespace it) (Actual a)
  where
    prefix = namespaceFirst $ _rocksDbTableIterNamespace it
{-# INLINE decIterKey #-}

data Checkpoint

foreign import ccall unsafe "rocksdb\\c.h rocksdb_checkpoint_object_create" 
    rocksdb_checkpoint_object_create :: C.RocksDBPtr -> Ptr CString -> IO (Ptr Checkpoint)

foreign import ccall unsafe "rocksdb\\c.h rocksdb_checkpoint_create"
    rocksdb_checkpoint_create :: Ptr Checkpoint -> CString -> CULong -> Ptr CString -> IO ()

foreign import ccall unsafe "rocksdb\\c.h rocksdb_checkpoint_object_destroy"
    rocksdb_checkpoint_object_destroy :: Ptr Checkpoint -> IO ()

foreign import ccall unsafe "rocksdb\\c.h rocksdb_free"
    rocksdb_free :: Ptr a -> IO ()

checked :: HasCallStack => String -> (Ptr CString -> IO a) -> IO a
checked whatWasIDoing act = alloca $ \errPtr -> do
    poke errPtr (nullPtr :: CString)
    r <- act errPtr
    err <- peek errPtr
    unless (err == nullPtr) $ do
        errStr <- B.packCString err
        let msg = unwords ["Data.CAS.RocksDB.checked: error while", whatWasIDoing <> ":", B8.unpack errStr]
        rocksdb_free err
        error msg
    return r

-- to unconditionally flush the WAL log before making the checkpoint, set logSizeFlushThreshold to zero. 
-- to *never* flush the WAL log, set logSizeFlushThreshold to maxBound :: CULong.
checkpointRocksDb :: HasCallStack => RocksDb -> CULong -> FilePath -> IO ()
checkpointRocksDb RocksDb { _rocksDbHandle = RocksDbHandle { _rocksDbHandleUnderlying = R.DB dbPtr _ } } logSizeFlushThreshold path = 
        let 
            mkCheckpointObject = 
                checked "creating checkpoint object" $ 
                    rocksdb_checkpoint_object_create dbPtr 
            mkCheckpoint cp =
                withCString path $ \path' -> 
                    checked "creating checkpoint" $ 
                        rocksdb_checkpoint_create cp path' logSizeFlushThreshold 
        in
            bracket mkCheckpointObject rocksdb_checkpoint_object_destroy mkCheckpoint 

foreign import ccall unsafe "rocksdb\\c.h rocksdb_approximate_sizes" 
    rocksdb_approximate_sizes
        :: C.RocksDBPtr 
        -> {- input: number of key ranges -} CInt 
        -> {- input: array of range start keys -} Ptr CString 
        -> {- input: array of range start key lengths -} Ptr CSize 
        -> {- input: array of range end keys -} Ptr CString 
        -> {- input: array of range end key lengths -} Ptr CSize 
        -> {- output: array of sizes -} Ptr CULong 
        -> {- errptr -} Ptr CString 
        -> IO ()

approxTableSizeRocksDb :: HasCallStack => RocksDb -> RocksDbTable k v -> IO CULong
approxTableSizeRocksDb RocksDb { _rocksDbHandle = RocksDbHandle { _rocksDbHandleUnderlying = R.DB dbPtr _ } } table = do
    alloca $ \rangeStartPtr ->
        alloca $ \rangeStartLengthPtr ->
        alloca $ \rangeEndPtr -> 
        alloca $ \rangeEndLengthPtr -> 
        alloca $ \sizePtr ->
        B.useAsCStringLen (namespaceFirst $ _rocksDbTableNamespace table) $ \(minKeyPtr, minKeyLen) ->
        B.useAsCStringLen (namespaceLast $ _rocksDbTableNamespace table) $ \(maxKeyPtr, maxKeyLen) -> do
            poke rangeStartPtr minKeyPtr
            poke rangeStartLengthPtr (fromIntegral minKeyLen :: CSize)
            poke rangeEndPtr maxKeyPtr
            poke rangeEndLengthPtr (fromIntegral maxKeyLen :: CSize)
            checked "calculating approximate table size" $ 
                rocksdb_approximate_sizes dbPtr 1 rangeStartPtr rangeStartLengthPtr rangeEndPtr rangeEndLengthPtr sizePtr 
            peek sizePtr

foreign import ccall unsafe "rocksdb\\c.h rocksdb_delete_range_cf"
    rocksdb_delete_range_cf
        :: C.RocksDBPtr
        -> C.WriteOptionsPtr 
        -> Ptr ColumnFamilyHandle
        -> CString {- min key -}
        -> CSize {- min key length -}
        -> CString {- max key length -}
        -> CSize {- max key length -}
        -> Ptr CString {- output: errptr -}
        -> IO ()

-- | Batch delete a range of keys in a table. 
-- Throws if the range of the *encoded keys* is not ordered (lower, upper).
deleteRange :: HasCallStack => RocksDbTable k v -> (k, k) -> IO ()
deleteRange table range = do
    let range' = over each (encKey table) range
    if fst range' >= snd range' then
        error "Data.CAS.RocksDB.deleteRange: range bounds not ordered according to codec"
    else do
        let defaultCf = _rocksDbHandleDefaultCf $ _rocksDbTableDb table
        let R.DB dbPtr _ = _rocksDbHandleUnderlying $ _rocksDbTableDb table
        R.withCWriteOpts R.defaultWriteOptions $ \optsPtr ->
            B.useAsCStringLen (fst range') $ \(minKeyPtr, minKeyLen) ->
            B.useAsCStringLen (snd range') $ \(maxKeyPtr, maxKeyLen) ->
            checked "Data.CAS.RocksDB.deleteRange" $ 
                rocksdb_delete_range_cf dbPtr optsPtr defaultCf minKeyPtr (fromIntegral minKeyLen :: CSize) maxKeyPtr (fromIntegral maxKeyLen :: CSize)

-- | Marshal a 'FilePath' (Haskell string) into a `NUL` terminated C string using
-- temporary storage.
-- On Linux, UTF-8 is almost always the encoding used.
-- When on Windows, UTF-8 can also be used, although the default for those devices is
-- UTF-16. For a more detailed explanation, please refer to
-- https://msdn.microsoft.com/en-us/library/windows/desktop/dd374081(v=vs.85).aspx.
withFilePath :: FilePath -> (CString -> IO a) -> IO a
withFilePath = GHC.withCString GHC.utf8
