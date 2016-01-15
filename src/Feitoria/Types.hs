{-# LANGUAGE BangPatterns #-}

module Feitoria.Types where

import Codec.MIME.Type

import qualified Data.ByteString as B

import Data.Time

import Data.Word

import Foreign.Ptr

import qualified Data.Text as T

import qualified Data.Vector as V

data TableHeader = TableHeader {
    tblProtVersion :: !Word64
  , tblTitle       :: !T.Text
  } deriving (Eq, Ord, Show)

data LazyTable = LazyTable {
    lazyTblHeader :: !TableHeader
  , lazyTblCols   :: [LazyColumn]
  } deriving (Eq, Ord, Show)

data MMapTable = MMapTable {
    mmapTblHeader          :: !TableHeader
  , mmapTblPtr             :: !(Ptr ())
  , mmapTblCols            :: [MMapColumn]
  , mmapTblNumColumns      :: !Word64
  , mmapTblNumRecords      :: !Word64
  , mmapTblColTblOffset    :: !Int
  , mmapTblStringLitOffset :: !Int
  , mmapTblArrayLitOffset  :: !Int
  , mmapTblBinaryLitOffset :: !Int
  } deriving (Eq, Ord, Show)

data ColumnHeader = ColumnHeader {
    colName       :: !T.Text
  , colType       :: !CellType
  , colArrayDepth :: !Word64
  } deriving (Eq, Ord, Show)

data LazyColumn = LazyColumn {
    lazyHeader :: !ColumnHeader
  , lazyCells  :: [Maybe Cell]
  } deriving (Eq, Ord, Show)

data MMapColumn = MMapColumn {
    mmapHeader :: !ColumnHeader
  , mmapOffset :: !Int
  } deriving (Eq, Ord, Show)

data CellType = TypeUInt
              | TypeInt
              | TypeDouble
              | TypeDateTime
              | TypeString
              | TypeBinary !MIMEType
              | TypeBoolean
              | TypeArray CellType
              deriving (Eq, Ord, Show)

data Cell = CellUInt !Word64
          | CellInt !Int
          | CellDouble !Double
          | CellDateTime !UTCTime
          | CellString !T.Text
          | CellBinary !B.ByteString
          | CellBoolean !Bool
          | CellArray !(V.Vector Cell)
          deriving (Eq, Ord, Show)
