{-# LANGUAGE BangPatterns #-}

module Feitoria.Types where

import Codec.MIME.Type

import qualified Data.ByteString as B

import Data.Time

import Data.Word

import Foreign.Ptr

import qualified Data.Text as T

import qualified Data.Vector as V

--data MMapTable = MMapTable {
--    mmapTblHeader          :: !TableHeader
--  , mmapTblPtr             :: !(Ptr ())
--  , mmapTblCols            :: [MMapColumn]
--  , mmapTblNumColumns      :: !Word64
--  , mmapTblNumRecords      :: !Word64
--  , mmapTblColTblOffset    :: !Int
--  , mmapTblStringLitOffset :: !Int
--  , mmapTblArrayLitOffset  :: !Int
--  , mmapTblBinaryLitOffset :: !Int
--  }
--
--data MMapColumn = MMapColumn {
--    mmapHeader :: !ColumnHeader
--  , mmapOffset :: !Int
--  }

data CellType = CellTypeUInt
              | CellTypeInt
              | CellTypeDouble
              | CellTypeDateTime
              | CellTypeString
              | CellTypeBinary MIMEType
              | CellTypeBoolean
              deriving (Eq, Ord, Show)

type Cell = Maybe CellData

data CellData = CellDataUInt     Word64
              | CellDataInt      Int
              | CellDataDouble   Double
              | CellDataDateTime UTCTime
              | CellDataBoolean  Bool
              | CellDataString   T.Text
              | CellDataBinary   B.ByteString
              | CellDataArray    (V.Vector CellData)
              deriving (Eq, Ord, Show)
