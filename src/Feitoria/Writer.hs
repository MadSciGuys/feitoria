{-# LANGUAGE OverloadedStrings
           , BangPatterns
           , TupleSections
           #-}

module Feitoria.Writer where

import           Codec.MIME.Type

import           Control.Exception

import           Control.Monad.Trans
import           Control.Monad.State.Strict

import           Data.Bits

import qualified Data.ByteString         as B
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy    as L

import qualified Data.Map.Strict         as M

import           Data.Monoid

import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T

import           Data.Time.Clock
import           Data.Time.Clock.POSIX

import qualified Data.Vector             as V

import           Data.Word

import           Feitoria.DList
import           Feitoria.IndexedBuilder
import           Feitoria.Types

import           Foreign.Marshal.Utils

import           System.Directory

import           System.IO
import           System.IO.Error
import           System.IO.Temp

-- For now we fold over each column in turn. This sucks for dealing with CSVs
-- though, since in that case we're lazy with respect to the rows. There should
-- be another writer later that deals with row-level laziness. This will require
-- one temporary file per column.

data TableWriter = TableWriter {
    twProtVersion :: Word64
  , twTitle       :: T.Text
  , twColumns     :: [ColumnWriter]
  }

data ColumnWriter = ColumnWriter {
    cwName       :: T.Text
  , cwType       :: CellType
  , cwArrayDepth :: Word64
  }

cellType :: CellType -> IndexedBuilder
cellType CellTypeUInt       = word8 0
cellType CellTypeInt        = word8 1
cellType CellTypeDouble     = word8 2
cellType CellTypeDateTime   = word8 3
cellType CellTypeString     = word8 4
cellType (CellTypeBinary m) = word8 5 <> textUtf8 (showMIMEType m)
cellType CellTypeBoolean    = word8 6

data Run = NullRun {
             nullRunLen :: !Word64
           , nullRunRow :: !Word64
           }
         | UniqueRun {
             uniqueRunLen  :: !Word16
           , uniqueRunRow  :: !Word64
           , uniqueRunVals :: DList CellData
           , uniqueRunLast :: !CellData
           }
         | ValueRun {
             valueRunLen :: !Word16
           , valueRunRow :: !Word64
           , valueRunVal :: !CellData
           }
         deriving (Show)

cellRuns :: [Cell] -> [Run]
cellRuns []             = []
cellRuns (Nothing : cs) = findRuns 0 (NullRun 1 0) cs
cellRuns ((Just c): cs) = findRuns 0 (ValueRun 1 0 c) cs

findRuns :: Word64 -> Run -> [Cell] -> [Run]
findRuns _  r []                                  = [r]
findRuns !n u@(NullRun l r) (Nothing : cs)        = findRuns (n+1) (NullRun (l+1) r) cs
findRuns !n u@(NullRun l r) ((Just c) : cs)       = u : findRuns (n+1) (ValueRun 1 (n+1) c) cs
findRuns !n u@(UniqueRun l r ds d) (Nothing :cs)  = u : findRuns (n+1) (NullRun 1 (n+1)) cs
findRuns !n u@(UniqueRun l r ds d) ((Just c) :cs)
    | d == c && l == 2 = (ValueRun 1 r (head (dlToList ds))) : findRuns (n+1) (ValueRun 2 n c) cs
    | d == c           = (UniqueRun (l-1) r (dlInit ds) d) : findRuns (n+1) (ValueRun 2 n c) cs
    | l == 32767       = u : findRuns (n+1) (ValueRun 1 (n+1) c) cs
    | otherwise        = findRuns (n+1) (UniqueRun (l+1) r (dlSnoc ds c) c) cs
findRuns !n u@(ValueRun l r d) (Nothing : cs)     = u : findRuns (n+1) (NullRun 1 (n+1)) cs
findRuns !n u@(ValueRun l r d) ((Just c) : cs)
    | l == 32767 = u : findRuns (n+1) (ValueRun 1 (n+1) c) cs
    | d == c     = findRuns (n+1) (ValueRun (l+1) r d) cs
    | l == 1     = findRuns (n+1) (UniqueRun 2 n (DList ((d:) . (c:))) c) cs
    | otherwise  = u : findRuns (n+1) (ValueRun 1 (n+1) c) cs

nullRun :: Word64 -> Word64 -> IndexedBuilder
nullRun l r = word16LE 0 <> word64LE r <> word64LE l

uniqueRun :: Word16 -> Word64 -> IndexedBuilder -> IndexedBuilder
uniqueRun l r b = word16LE ((bit 15) .|. l) <> word64LE r <> b

valueRun :: Word16 -> Word64 -> IndexedBuilder -> IndexedBuilder
valueRun l r b = word16LE l <> word64LE r <> b

-- The offsets are relative to the column table.
columnHeader :: (ColumnWriter, Word64) -> IndexedBuilder
columnHeader ((ColumnWriter n t d), o) = textUtf8 n
                                      <> cellType t
                                      <> word64LE d
                                      <> word64LE o

eitherMaybe :: Either e () -> Maybe e
eitherMaybe (Right ()) = Nothing
eitherMaybe (Left e)   = Just e

-- Resets the source handle to position 0.
appHandle :: MonadIO m => Handle -> Handle -> m ()
appHandle t a = liftIO $ hSeek a AbsoluteSeek 0 >> L.hGetContents a >>= L.hPut t
