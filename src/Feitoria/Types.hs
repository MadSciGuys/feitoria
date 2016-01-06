module Feitoria.Types where

import Codec.MIME.Type

import qualified Data.ByteString as B

import Data.Time

import Data.Word

import Foreign.Ptr

import qualified Data.Text as T

data TableHeader = TableHeader {
    tblProtVersion :: Word64
  , tblTitle       :: T.Text
  , tblNumColumns  :: Word64
  , tblNumRecords  :: Word64
  , tblColumns     :: ColumnSet
  } deriving (Eq, Ord, Show)

data LazyTable = LazyTable {
    lazyTblHeader :: TableHeader
  , lazyTblCols   :: [LazyColumn]
  } deriving (Eq, Ord, Show)

data MMapTable = MMapTable {
    mmapTblHeader          :: TableHeader
  , mmapTblPtr             :: Ptr ()
  , mmapTblStringLitOffset :: Int
  , mmapTblArrayLitOffset  :: Int
  , mmapTblBinaryLitOffset :: Int
  , mmapTblCols            :: [MMapColumn]
  } deriving (Eq, Ord, Show)

data ColumnHeader = Column {
    colName       :: T.Text
  , colType       :: CellType
  , colArrayDepth :: Word64
  , colMimeGuess  :: Maybe MIMEType
  } deriving (Eq, Ord, Show)

data LazyColumn = LazyColumn {
    lazyHeader :: ColumnHeader
  , lazyCells  :: [Maybe Cell]
  }

data MMapColumn = MMapColumn {
    mmapHeader :: ColumnHeader
  , mmapOffset :: Int
  }

data CellType = UInt
              | Int
              | Double
              | DateTime
              | String
              | Binary
              | Boolean
              deriving (Eq, Ord, Show)

data Cell = UInt Word64
          | Int Int
          | Double Double
          | DateTime UTCTime
          | String T.Text
          | Binary B.ByteString
          | Boolean Bool
          deriving (Eq, Ord, Show)
