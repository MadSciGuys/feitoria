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

data TableWriterState = TableWriterState {
    stringLitCache :: M.Map T.Text Word64
  , arrayLitCache  :: M.Map (V.Vector CellData) Word64
  , binaryLitCache :: M.Map B.ByteString Word64
  , validColLen    :: Maybe Word64
  , colHeaderPH    :: (FilePath, PosHandle)
  , columnPH       :: (FilePath, PosHandle)
  , stringLitPH    :: (FilePath, PosHandle)
  , binaryLitPH    :: (FilePath, PosHandle)
  , arrayLitPH     :: (FilePath, PosHandle)
  , columnOffsets  :: DList Word64
  }

type TableWriterM = StateT TableWriterState IO

initTableWriterState :: FilePath -> IO (Either IOError TableWriterState)
initTableWriterState tfp = try $ TableWriterState M.empty M.empty M.empty Nothing
    <$> tf
    <*> tf
    <*> tf
    <*> tf
    <*> tf
    <*> pure (DList id)
    where tf = openBinaryTempFile tfp "feit" >>= (\(fp, h) -> (fp,) <$> mkPosHandle h)

rmPH :: MonadIO m => TableWriterState
                  -> (TableWriterState -> (FilePath, PosHandle))
                  -> m ()
rmPH s a = liftIO $ do
  hClose (phHandle (snd (a s)))
  removeFile (fst (a s))

eitherMaybe :: Either e () -> Maybe e
eitherMaybe (Right ()) = Nothing
eitherMaybe (Left e)   = Just e

cleanupTableWriterState :: TableWriterState -> IO (Maybe IOError)
cleanupTableWriterState s = eitherMaybe <$> try cleanTemp
    where cleanTemp              = do
            rmPH s colHeaderPH
            rmPH s columnPH
            rmPH s stringLitPH
            rmPH s binaryLitPH
            rmPH s arrayLitPH

emptyCaches :: TableWriterM ()
emptyCaches = modify'
    (\s -> s { stringLitCache = M.empty
             , arrayLitCache  = M.empty
             , binaryLitCache = M.empty
             })

stringLitCacheInsert :: T.Text -> Word64 -> TableWriterM ()
stringLitCacheInsert t i = modify'
    (\s -> s { stringLitCache = M.insert t i (stringLitCache s) })

arrayLitCacheInsert :: (V.Vector CellData) -> Word64 -> TableWriterM ()
arrayLitCacheInsert v i = modify'
    (\s -> s { arrayLitCache = M.insert v i (arrayLitCache s) })

binaryLitCacheInsert :: B.ByteString -> Word64 -> TableWriterM ()
binaryLitCacheInsert b i = modify'
    (\s -> s { binaryLitCache = M.insert b i (binaryLitCache s) })

-- | Returns a builder writing either the value itself or a pointer to a
--   literal, depending on the data type.
writeCellData :: CellData -> TableWriterM IndexedBuilder
writeCellData (CellDataUInt w)     = return (word64LE w)
writeCellData (CellDataInt i)      = return (int64LE (fromIntegral i))
writeCellData (CellDataDouble d)   = return (doubleLE d)
writeCellData (CellDataDateTime t) = return ((word64LE .  fromIntegral . floor . utcTimeToPOSIXSeconds) t)
writeCellData (CellDataBoolean b)  = return (word64LE (fromBool b))
writeCellData (CellDataString s)   = do
    sc <- gets stringLitCache
    let addString = do
            (fp, ph) <- gets stringLitPH
            let w = fromIntegral $ phPos ph
            ph' <- hPutIndexedBuilderM ph (textUtf8 s)
            modify' (\t -> t { stringLitPH = (fp, ph') })
            stringLitCacheInsert s w
            return w
    case M.lookup s sc of (Just w) -> return (word64LE w)
                          Nothing  -> word64LE <$> addString
writeCellData (CellDataBinary b)   = do
    bc <- gets binaryLitCache
    let addBin = do
            (fp, ph) <- gets binaryLitPH
            let w = fromIntegral $ phPos ph
            ph' <- hPutIndexedBuilderM ph (byteStringPascalString b)
            modify' (\t -> t { binaryLitPH = (fp, ph') })
            binaryLitCacheInsert b w
            return w
    case M.lookup b bc of (Just w) -> return (word64LE w)
                          Nothing  -> word64LE <$> addBin
writeCellData (CellDataArray v)    = do
    ac <- gets arrayLitCache
    let addVec = do
            (fp, ph) <- gets arrayLitPH
            let w  = fromIntegral $ phPos ph
                lb = word64LE (fromIntegral (V.length v))
            cb  <- (V.foldl (<>) mempty) <$> V.mapM writeCellData v
            ph' <- hPutIndexedBuilderM ph (lb <> cb)
            modify' (\t -> t { arrayLitPH = (fp, ph') })
            arrayLitCacheInsert v w
            return w
    case M.lookup v ac of (Just w) -> return (word64LE w)
                          Nothing  -> word64LE <$> addVec

-- | Returns the index of the array start.
writeArrayLit :: DList CellData -> TableWriterM IndexedBuilder
writeArrayLit = writeCellData . CellDataArray . V.fromList . dlToList

addColumnOffset :: TableWriterM ()
addColumnOffset = modify
    (\s -> s { columnOffsets = dlSnoc (columnOffsets s) (fromIntegral (phPos (snd (columnPH s)))) })

validateColLen :: Word64 -> TableWriterM Bool
validateColLen l = do
    vl <- gets validColLen
    case vl of Nothing  -> modify' (\s -> s { validColLen = Just l }) >> return True
               (Just v) -> return (v == l)

writeRuns :: Word64 -> [Run] -> TableWriterM Word64
writeRuns !n []                        = return n
writeRuns !n ((NullRun l r):rs)        = do
    (fp, ph) <- gets columnPH
    ph' <- hPutIndexedBuilderM ph $ nullRun l r
    modify' (\s -> s { columnPH = (fp, ph') })
    writeRuns (n+1) rs
writeRuns !n ((UniqueRun l r vs _):rs) = do
    (fp, ph) <- gets columnPH
    b   <- writeArrayLit vs
    ph' <- hPutIndexedBuilderM ph $ uniqueRun l r b
    modify' (\s -> s { columnPH = (fp, ph') })
    writeRuns (n+1) rs
writeRuns !n ((ValueRun l r v):rs)     = do
    (fp, ph) <- gets columnPH
    b        <- writeCellData v
    ph' <- hPutIndexedBuilderM ph $ valueRun l r b
    modify' (\s -> s { columnPH = (fp, ph') })
    writeRuns (n+1) rs

writeColumnRuns :: [Run] -> TableWriterM Bool
writeColumnRuns rs = addColumnOffset >> writeRuns 0 rs >>= validateColLen

writeColumns :: [(T.Text, [Run])] -> TableWriterM (Maybe T.Text)
writeColumns []          = return Nothing
writeColumns ((n, rs):cs) = do
    g <- writeColumnRuns rs
    if g then writeColumns cs
         else return (Just n)

catColumnHeader :: [ColumnWriter] -> TableWriterM ()
catColumnHeader cs = do
    os <- gets (dlToList . columnOffsets)
    (fp, ph) <- gets colHeaderPH
    ph' <- hPutIndexedBuilderM ph (mconcat (map columnHeader (zip cs os)))
    modify' (\s -> s { colHeaderPH = (fp, ph') })

-- Resets the source handle to position 0.
appHandle :: MonadIO m => Handle -> Handle -> m ()
appHandle t a = liftIO $ hSeek a AbsoluteSeek 0 >> L.hGetContents a >>= L.hPut t

catTable :: TableWriter -> Handle -> TableWriterM ()
catTable (TableWriter v t cs) h = do
    s <- get
    colHeaderH <- gets (phHandle . snd . colHeaderPH)
    columnH    <- gets (phHandle . snd . columnPH)
    stringLitH <- gets (phHandle . snd . stringLitPH)
    binaryLitH <- gets (phHandle . snd . binaryLitPH)
    arrayLitH  <- gets (phHandle . snd . arrayLitPH)
    colHeaderSize <- gets (phPos . snd . colHeaderPH)
    columnSize    <- gets (phPos . snd . columnPH)
    stringLitSize <- gets (phPos . snd . stringLitPH)
    binaryLitSize <- gets (phPos . snd . binaryLitPH)
    let (IndexedBuilder b i) = byteString "w\01\01\01"
                            <> word64LE v
                            <> textUtf8 t
                            <> word64LE (fromIntegral (length cs))
        tableHeaderSize      = i + (8 * 4) -- Header so far, plus four more pointers.
        colTableOffset       = tableHeaderSize + colHeaderSize
        stringTableOffset    = colTableOffset + columnSize
        binaryTableOffset    = stringTableOffset + stringLitSize
        arrayTableOffset     = binaryTableOffset + binaryLitSize
    liftIO $ B.hPutBuilder h (b <> B.word64LE (fromIntegral colTableOffset)
                                <> B.word64LE (fromIntegral stringTableOffset)
                                <> B.word64LE (fromIntegral binaryTableOffset)
                                <> B.word64LE (fromIntegral arrayTableOffset))
    appHandle h colHeaderH
    rmPH s colHeaderPH
    appHandle h columnH
    rmPH s columnPH
    appHandle h stringLitH
    rmPH s stringLitPH
    appHandle h binaryLitH
    rmPH s binaryLitPH
    appHandle h arrayLitH
    rmPH s arrayLitPH

writeTable :: -- | The table to write to disk.
              TableWriter
              -- | A strict list of lazy lists of column cells.
           -> [[Cell]]
              -- | A directory in which temporary files may be created.
           -> FilePath
              -- | File name to which the table will be written.
           -> FilePath
              -- | Provide 'Nothing' on success or 'Just' an error message. Make this better later.
           -> IO (Maybe IOError)
writeTable table cells tmpdir outfile
    | (length (twColumns table)) /= (length cells) = return
        (Just (userError "TableWriter and cell list have different column numbers."))
    | otherwise = do
        h  <- openBinaryFile outfile WriteMode
        is <- initTableWriterState tmpdir
        case is of (Left e)  -> return $ Just e
                   (Right s) -> eitherMaybe <$> try (evalStateT (writeTable' h) s)
        where writeTable' h = do
                bc <- writeColumns $ zip (map cwName (twColumns table)) (map cellRuns cells)
                case bc of (Just e) -> emptyCaches >> throw (userError ((T.unpack e) ++ " has the wrong number of rows."))
                           Nothing  -> emptyCaches >> catColumnHeader (twColumns table) >> catTable table h
                                                   >> liftIO (hClose h)
