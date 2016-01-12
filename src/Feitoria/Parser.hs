{-# LANGUAGE OverloadedStrings
           , BangPatterns
           #-}

module Feitoria.Parser where

import Codec.MIME.Type
import Codec.MIME.Parse

import Control.Monad.Trans.Class
import Control.Monad.Trans.State

import Data.Binary.Get
import Data.Binary.Put

import Data.Map as M

import qualified Data.ByteString as B

import Data.Time

import Data.Word

import Foreign.Ptr

import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import qualified Data.Vector as V

import Feitoria.Types

type LazyTableWriter = StateT LazyTableState PutM ()

type CellCC = [Cell] -> [Cell]

data LazyTableState = LazyTableState {
    stringLitCache :: M.Map T.Text Int
  , arrayLitCache  :: M.Map (V.Vector Cell) Int
  , binaryLitCache :: M.Map B.ByteString Int
  , currentRun     :: !LazyTableRun
  }

data LazyTableRun = NullRun {
                      nullRunLen :: !Int
                    , nullRunRow :: !Int
                    }
                  | UniqueRun {
                      uniqueRunLen  :: !Int
                    , uniqueRunRow  :: !Int
                    , uniqueRunVals :: CellCC
                    , uniqueRunLast :: !Cell
                    }
                  | ValueRun {
                      valueRunLen :: !Int
                    , valueRunRow :: !Int
                    , valueRunVal :: !Cell
                    }

putTable :: LazyTable -> FilePath -> IO ()
putTable table fp = undefined

putLazyTable :: LazyTable -> LazyTableWriter
putLazyTable (LazyTable header cols) = do
    lift (putTableHeader header)
    -- write all of the headers, and then write all of the cells
    mapM_ (lift . putColumnHeader . lazyHeader) cols
    mapM_ (putCells . lazyCells) cols

putTableHeader :: TableHeader -> Put
putTableHeader t = do
    putByteString "FEITORIA"
    putWord64le (tblProtVersion t)
    putStrictByteString (T.encodeUtf8 (tblTitle t))
    putWord64le (tblNumColumns t)
    putWord64le (tblNumRecords t)

putColumnHeader :: ColumnHeader -> Put
putColumnHeader c = do
    putStrictByteString (T.encodeUtf8 (colName c))
    putCellType (colType c)
    putWord64le (colArrayDepth c)
    putStrictByteString (T.encodeUtf8 (showMIMEType (colMIMEType c)))
    putWord64le (fromIntegral (colStart c))

-- | This just writes the frames in linear order right now.
putCells :: [Maybe Cell] -> LazyTableWriter
putCells []     = return ()
putCells (c:cs) = put (initCache c) >> mapM_ runFrames cs
    where initCache Nothing  = LazyTableState M.empty
                                              M.empty
                                              M.empty
                                              (NullRun 1 0)
          initCache (Just c) = LazyTableState M.empty
                                              M.empty
                                              M.empty
                                              (ValueRun 1 0 c)

runFrames :: Maybe Cell -> LazyTableWriter
runFrames c = do
    cr <- gets currentRun
    case cr of (NullRun l r)        -> runNulls c l r
               (UniqueRun l r vs v) -> runUniques c l r vs v
               (ValueRun l r v)     -> runValues c l r v

runNulls :: Maybe Cell -> Int -> Int -> LazyTableWriter
runNulls Nothing l r  = modify'
    (\s -> s { currentRun = (currentRun s) { nullRunLen = (l + 1) }})
runNulls (Just c) l r = putFrame >> modify'
    (\s -> s { currentRun = ValueRun 1 (r + l) c })

runUniques :: Maybe Cell -> Int -> Int -> CellCC -> Cell -> LazyTableWriter
runUniques Nothing l r _ _ = putFrame >> modify'
    (\s -> s { currentRun = NullRun 1 (r + l) })
runUniques (Just c) l r vs v
    -- turn it into a value run if we see two in a row
    | c == v = modify'
        -- pull the last value off of the unique run
        (\s -> s {
            currentRun = (currentRun s) {
                uniqueRunVals = tail . vs
              , uniqueRunLen  = l - 1
              }
          }
        )
        -- output the unique run
        >> putFrame
        -- create a new value run with our new cells
        >> modify' (\s -> s { currentRun = ValueRun 2 (r + l - 1) c })
    -- keep going with the current unique run
    | otherwise = modify'
        (\s -> s {
            currentRun = (currentRun s) {
                uniqueRunLen  = l + 1
              , uniqueRunVals = vs . (c:)
              , uniqueRunLast = c
              }
          }
        )

runValues :: Maybe Cell -> Int -> Int -> Cell -> LazyTableWriter
runValues Nothing l r _ = putFrame >> modify'
    (\s -> s { currentRun = NullRun 1 (r + l)})
runValues (Just c) l r v
    -- keep our current value run if the next value is the same as our runs
    | c == v = modify'
        (\s -> s { currentRun = (currentRun s) { valueRunLen = l + 1 } })
    -- we just saw a different value
    -- if this is a singleton value run, convert it into a unique run
    | l == 1 = modify' (\s -> s { currentRun = UniqueRun 2 r ((c:) . (v:)) c })
    -- output our current value run and make a new singleton value run
    | otherwise = putFrame >> modify'
        (\s -> s { currentRun = ValueRun 1 (r + l) c })

putFrame :: LazyTableWriter
putFrame = undefined

-- putColumnData8 :: Column -> Put
-- putColumnData8 = undefined

putCellType :: CellType -> Put
putCellType TypeUInt     = putWord8 0
putCellType TypeInt      = putWord8 1
putCellType TypeDouble   = putWord8 2
putCellType TypeDateTime = putWord8 3
putCellType TypeString   = putWord8 4
putCellType TypeBinary   = putWord8 5
putCellType TypeBoolean  = putWord8 6

putStrictByteString :: B.ByteString -> Put
putStrictByteString = (putWord8 0 <*) . putByteString
