{-# LANGUAGE OverloadedString #-}

module Feitoria.Parser where

import Codec.MIME.Type
import Codec.MIME.Parse

import Data.Binary.Get

import qualified Data.ByteString as B

import Data.Time

import Data.Word

import Foreign.Ptr

import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Feitoria.Types

putLazyTable :: LazyTable -> FilePath -> IO ()

putTableHeader :: Table -> Put
putTableHeader t = do
    putByteString "FEITORIA"
    putWord64le (tblProtVersion t)
    putNullByteString (T.encodeUtf8 (tblTitle t))
    putWord64le (tblNumColumns t)
    putWord64le (tblNumRecords t)
    putWord64le (fromIntegral (tblStringLit t))
    putWord64le (fromIntegral (tblArrayLit t))
    putWord64le (fromIntegral (tblBinaryLit t))
    mapM_ putColumnHeader (tblColumns t)
    mapM_ putColumnData (tblColumns t)

putColumnHeader :: Column -> Put
putColumnHeader c = do
    putNullByteString (T.encodeUtf8 (colName c))
    putCellType (colType c)
    putWord64le (colArrayDepth c)
    putNullByteString ((T.encodeUtf8 . showMIMEType) <$> (colMimeGuess c)) -- Let's try this later, check out the STG: (T.encodeUtf8 <$> showMIMEType <$> colMimeGuess c)
    putWord64le (fromIntegral (colStart c))

putColumnData :: Ptr () -> Column -> Put
putColumnData = 

putColumnData16 :: Column -> Put
putColumnData16 = 

putColumnData8 :: Column -> Put
putColumnData8 = undefined

putCellType :: CellType -> Put
putCellType UInt     = putWord8 0
putCellType Int      = putWord8 1
putCellType Double   = putWord8 2
putCellType DateTime = putWord8 3
putCellType String   = putWord8 4
putCellType Binary   = putWord8 5
putCellType Boolean  = putWord8 6

putStrictByteString :: B.ByteString -> Put
putStricByteString = (putWord8 0 <*) . putByteString
