{-# LANGUAGE OverloadedStrings
           , BangPatterns
           #-}

module Feitoria.Reader where

import qualified Data.ByteString.Lazy as BL

import Control.Exception

import Codec.MIME.Type
import Codec.MIME.Parse

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.State

import Data.Binary.Get
import Data.Binary.Put

import Data.Bits

import Data.Bool

import qualified Data.Map as M

import Data.Maybe

import qualified Data.ByteString as B

import Data.Time
import Data.Time.Clock.POSIX

import Data.Word

import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc
import Foreign.Marshal.Unsafe (unsafeLocalState)

import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import qualified Data.Vector as V

import Feitoria.Types

import System.Directory

import System.IO

import System.IO.Temp

-- openMmapTable :: FilePath
--               -> IO MMapTable
-- openMmapTable = do

data CompleteTableHeader = CompleteTableHeader {
      cthTableHeader :: TableHeader
    , cthColCount :: Word64
    , cthRecCount :: Word64
    , cthColTableOff :: Int
    , cthStrTableOff :: Int
    , cthArrTableOff :: Int
    , cthBinTableOff :: Int
    , cthMMapCols :: [MMapColumn]
    } deriving Show

getCompleteTableHeader :: Get CompleteTableHeader
getCompleteTableHeader = do
    "w\01\01\01" <- getByteString 4 -- TODO
    tableHeader <- getTableHeader
    colCount <- getWord64le
    CompleteTableHeader <$> pure tableHeader
                        <*> pure colCount
                        <*> getWord64le
                        <*> fmap fromIntegral getWord64le
                        <*> fmap fromIntegral getWord64le
                        <*> fmap fromIntegral getWord64le
                        <*> fmap fromIntegral getWord64le
                        <*> getMMapColumns colCount

getMMapColumns :: Word64 -> Get [MMapColumn]
getMMapColumns cols = replicateM (fromIntegral cols) getMMapColumn

getMMapColumn :: Get MMapColumn
getMMapColumn =
    MMapColumn <$> getColumnHeader
               <*> fmap fromIntegral getWord64le

getColumnHeader :: Get ColumnHeader
getColumnHeader =
    ColumnHeader <$> getTextUtf8
                 <*> getCellType
                 <*> getWord64le

getCellType :: Get CellType
getCellType = do
    ct <- getWord8
    case ct of
        0 -> return TypeUInt
        1 -> return TypeInt
        2 -> return TypeDouble
        3 -> return TypeDateTime
        4 -> return TypeString
        5 -> TypeBinary <$> undefined -- fmap (fromMaybe parseMIMEType) getTextUtf8 -- TODO -- getTextUtf8
        6 -> return TypeBoolean

getTableHeader :: Get TableHeader
getTableHeader =
    TableHeader <$> getWord64le
                <*> getTextUtf8

getTextUtf8 :: Get T.Text
getTextUtf8 = T.decodeUtf8 <$> getNullByteString

getNullByteString :: Get B.ByteString
getNullByteString = BL.toStrict <$> getLazyByteStringNul
