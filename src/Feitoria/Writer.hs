{-# LANGUAGE OverloadedStrings
           , BangPatterns
           #-}

module Feitoria.Writer where

import           Codec.MIME.Type

import           Control.Monad.Trans
import           Control.Monad.State.Strict

import           Data.Bits

import qualified Data.ByteString    as B

import qualified Data.Map.Strict    as M

import           Data.Monoid

import qualified Data.Text          as T
import qualified Data.Text.Encoding as T

import           Data.Time.Clock
import           Data.Time.Clock.POSIX

import qualified Data.Vector        as V

import           Data.Word

import           Feitoria.DList
import           Feitoria.IndexedBuilder
import           Feitoria.Types

import           Foreign.Marshal.Utils

import           System.IO

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
  , cwCells      :: [Cell]
  }

renderCellType :: CellType -> IndexedBuilder
renderCellType CellTypeUInt       = word8 0
renderCellType CellTypeInt        = word8 1
renderCellType CellTypeDouble     = word8 2
renderCellType CellTypeDateTime   = word8 3
renderCellType CellTypeString     = word8 4
renderCellType (CellTypeBinary m) = word8 5 <> textUtf8 (showMIMEType m)
renderCellType CellTypeBoolean    = word8 6

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

uniqueRun :: Word16 -> Word64 -> Word64 -> IndexedBuilder
uniqueRun l r vi = word16LE ((bit 15) .|. l) <> word64LE r <> word64LE vi

valueRun :: Word16 -> Word64 -> IndexedBuilder -> IndexedBuilder
valueRun l r b = word16LE l <> word64LE r <> b

data TableWriterState = TableWriterState {
    stringLitCache :: M.Map T.Text Word64
  , arrayLitCache  :: M.Map (V.Vector CellData) Word64
  , binaryLitCache :: M.Map B.ByteString Word64
  , validColLen    :: Maybe Word64
  , colCount       :: Word64
  , colHeaderPH    :: (FilePath, PosHandle)
  , columnPH       :: (FilePath, PosHandle)
  , stringLitPH    :: (FilePath, PosHandle)
  , binaryLitPH    :: (FilePath, PosHandle)
  , arrayLitPH     :: (FilePath, PosHandle)
  , columnOffsets  :: DList Word64
  }

type TableWriterM = StateT TableWriterState IO

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
writeCellData (CellDataString s)   = do
    sc <- gets stringLitCache
    let addString = do
            (_, ph) <- gets stringLitPH
            let w = fromIntegral $ phPos ph
            hPutIndexedBuilderM ph (textUtf8 s)
            stringLitCacheInsert s w
            return w
    case M.lookup s sc of (Just w) -> return (word64LE w)
                          Nothing  -> word64LE <$> addString
writeCellData (CellDataBinary b)   = do
    bc <- gets binaryLitCache
    let addBin = do
            (_, ph) <- gets binaryLitPH
            let w = fromIntegral $ phPos ph
            hPutIndexedBuilderM ph (byteStringPascalString b)
            binaryLitCacheInsert b w
            return w
    case M.lookup b bc of (Just w) -> return (word64LE w)
                          Nothing  -> word64LE <$> addBin
writeCellData (CellDataBoolean b)  = return (word64LE (fromBool b))

-- | Returns the index of the array start.
writeArrayLit :: DList CellData -> TableWriterM Word64
writeArrayLit cs = do
    ac <- gets arrayLitCache
    let cl       = dlToList cs
        cv       = V.fromList cl
        addArray = do
            (_, ph) <- gets arrayLitPH
            let w  = fromIntegral $ phPos ph
                lb = word64LE (fromIntegral (V.length cv))
            cb <- mconcat <$> mapM writeCellData cl
            hPutIndexedBuilderM ph (lb <> cb)
            arrayLitCacheInsert cv w
            return w
    case M.lookup cv ac of (Just w) -> return w
                           Nothing  -> addArray

addColumnOffset :: TableWriterM ()
addColumnOffset = modify
    (\s -> s { columnOffsets = dlSnoc (columnOffsets s) (fromIntegral (phPos (snd (columnPH s)))) })

validateColLen :: Word64 -> TableWriterM Bool
validateColLen l = do
    vl <- gets validColLen
    case vl of Nothing  -> modify' (\s -> s { validColLen = Just l }) >> return True
               (Just v) -> return (v == l)

writeRun :: Word64 -> Run -> TableWriterM Word64
writeRun !n (NullRun l r)        = do
    (_, ph) <- gets columnPH
    hPutIndexedBuilderM ph $ nullRun l r
    return (n+1)
writeRun !n (UniqueRun l r vs _) = do
    (_, ph) <- gets columnPH
    vi      <- writeArrayLit vs
    hPutIndexedBuilderM ph $ uniqueRun l r vi
    return (n+1)
writeRun !n (ValueRun l r v)     = do
    (_, ph) <- gets columnPH
    b       <- writeCellData v
    hPutIndexedBuilderM ph $ valueRun l r b
    return (n+1)

writeColumnRuns :: [Run] -> TableWriterM Bool
writeColumnRuns rs = addColumnOffset >> foldM writeRun 0 rs >>= validateColLen

--------------------------------------------------------------------------------
--old:

--import qualified Data.ByteString.Lazy as BL
--
--import Control.Exception
--
--import Codec.MIME.Type
--import Codec.MIME.Parse
--
--import Control.Monad
--import Control.Monad.IO.Class
--import Control.Monad.Trans.Class
--import Control.Monad.Trans.State
--
--import Data.Binary.Get
--import Data.Binary.Put
--
--import Data.Bits
--
--import Data.Bool
--
--import qualified Data.Map as M
--
--import Data.Maybe
--
--import qualified Data.ByteString as B
--
--import Data.Time
--import Data.Time.Clock.POSIX
--
--import Data.Word
--
--import Foreign.Ptr
--import Foreign.Storable
--import Foreign.Marshal.Alloc
--import Foreign.Marshal.Unsafe (unsafeLocalState)
--
--import qualified Data.Text as T
--import qualified Data.Text.Encoding as T
--
--import qualified Data.Vector as V
--
--import Feitoria.Types
--
--import System.Directory
--
--import System.IO
--
--import System.IO.Temp
--
--
--
--putTable :: LazyTable -- ^ The table to write to disk.
--         -> FilePath  -- ^ A directory in which temporary files may be created.
--         -> FilePath  -- ^ File name to which the table will be written.
--         -> IO ()
--putTable table tempdir outfile = initState
--                             >>= void . runStateT (putTable' table outfile)
--    -- Please make this even more exception safe.
--    where initState = bracketOnError acqFPs (mapM_ closeTempFP) mkInitState
--          acqFPs = replicateM 5 (openBinaryTempFile tempdir tnf)
--          tnf = "libfeit"
--          mkInitState [ch,ct,st,at,bt] = return $
--                    LazyTableState M.empty
--                                   M.empty
--                                   M.empty
--                                   UninitializedRun
--                                   0
--                                   Nothing
--                                   0
--                                   ch
--                                   ct
--                                   st
--                                   at
--                                   bt
--                                   id
--
---- | Close a temp file's 'Handle' and delete the file.
--closeTempFP :: (FilePath, Handle) -> IO ()
--closeTempFP (f, h) = hClose h >> removeFile f
--
---- | If it is possible for putCells to fail silently somehow, we need to check
----   the length of the list of headers and compare it to the length of the list
----   of cell offsets.
--putTable' :: LazyTable -> FilePath -> LazyTableWriter
--putTable' t o = do
--    outfile     <- liftIO $ openFile o WriteMode
--    liftIO $ putHandle outfile $ putTableHeader (lazyTblHeader t)
--    mapM_ (putColumnCells . lazyCells) (lazyTblCols t)
--    offsets     <- ($ []) <$> gets columnOffsets
--    let colheaders = zipWith MMapColumn (map lazyHeader (lazyTblCols t)) offsets
--    mapM_ (putInStateFP colHeaderFP . putColumnHeader) colheaders
--    -- get all the temporary files
--    ch@(chfp, chh) <- gets colHeaderFP
--    ct@(ctfp, cth) <- gets columnFP
--    st@(stfp, sth) <- gets stringLitFP
--    at@(atfp, ath) <- gets arrayLitFP
--    bt@(btfp, bth) <- gets binaryLitFP
--    -- get the size of all of the temporary files
--    th_len <- liftIO $ fromIntegral <$> hTell outfile
--    ch_len <- liftIO $ fromIntegral <$> hTell chh
--    ct_len <- liftIO $ fromIntegral <$> hTell cth
--    st_len <- liftIO $ fromIntegral <$> hTell sth
--    at_len <- liftIO $ fromIntegral <$> hTell ath
--    bt_len <- liftIO $ fromIntegral <$> hTell bth
--    liftIO $ do hSeek chh AbsoluteSeek 0
--                hSeek cth AbsoluteSeek 0
--                hSeek sth AbsoluteSeek 0
--                hSeek ath AbsoluteSeek 0
--                hSeek bth AbsoluteSeek 0
--    col_count   <- gets colCount
--    rec_count   <- fromMaybe 0 <$> gets validColLen
--    -- get the offsets for the various tables
--    let -- 4 offset uint64s + 2 col/row count uint64s = 48
--        ct_offset = th_len + ch_len + 48
--        st_offset = ct_offset + ct_len
--        at_offset = st_offset + st_len
--        bt_offset = at_offset + at_len
--    -- Here's where we actually build the final output file:
--    liftIO $ do putHandle outfile $ putWord64le col_count
--                putHandle outfile $ putWord64le rec_count
--                putHandle outfile $ putWord64le ct_offset
--                putHandle outfile $ putWord64le st_offset
--                putHandle outfile $ putWord64le at_offset
--                putHandle outfile $ putWord64le bt_offset
--                catHandles outfile chh
--                closeTempFP ch
--                catHandles outfile cth
--                closeTempFP ct
--                catHandles outfile sth
--                closeTempFP st
--                catHandles outfile ath
--                closeTempFP at
--                catHandles outfile bth
--                closeTempFP bt
--                hClose outfile
--
---- | Assumes the source handle is finite. Please make this exception safe.
--catHandles :: Handle -> Handle -> IO ()
--catHandles sink source = B.hGetContents source >>= B.hPut sink
--
--putTableHeader :: TableHeader -> Put
--putTableHeader t = do
--    putByteString "w\01\01\01"
--    putWord64le (tblProtVersion t)
--    putTextUtf8 (tblTitle t)
--
--putColumnHeader :: MMapColumn -> Put
--putColumnHeader (MMapColumn c o) = do
--    putTextUtf8 (colName c)
--    putCellType (colType c)
--    putWord64le (colArrayDepth c)
--    putWord64le (fromIntegral o)
--
---- | This just writes the frames in linear order right now.
--putColumnCells :: [Maybe Cell] -> LazyTableWriter
--putColumnCells [] = finalizeCol
--putColumnCells cs = do
--    offset <- fromIntegral <$> (gets (snd . columnFP) >>= liftIO . hTell)
--    modify' (\s -> s { columnOffsets = (columnOffsets s) . (offset:) })
--    mapM_ runFrames cs
--    putFrame
--    finalizeCol
--
---- | Get ready to write the next column. Find a better way to throw an error in
----   this monad stack.
--finalizeCol :: LazyTableWriter
--finalizeCol = do
--    vcl <- gets validColLen
--    ccl <- gets currentColLen
--    let vcl' = maybe (Just ccl)
--                     (\v -> if v == ccl then Just v
--                            else error "columns must have same length")
--                     vcl
--    modify' (\s -> s { currentRun = UninitializedRun
--                     , currentColLen = 0
--                     , validColLen = vcl'
--                     , colCount = (colCount s) + 1
--                     })
--
--runFrames :: Maybe Cell -> LazyTableWriter
--runFrames c = do
--    modify' (\s -> s { currentColLen = (currentColLen s) + 1 })
--    cr <- gets currentRun
--    case cr of (NullRun l r)        -> runNulls c l r
--               (UniqueRun l r vs v) -> runUniques c l r vs v
--               (ValueRun l r v)     -> runValues c l r v
--               UninitializedRun     -> firstRun c
--
--firstRun :: Maybe Cell -> LazyTableWriter
--firstRun Nothing  = modify' (\s -> s { currentRun = NullRun 1 0 })
--firstRun (Just c) = modify' (\s -> s { currentRun = ValueRun 1 0 c})
--
--runNulls :: Maybe Cell -> Word64 -> Word64 -> LazyTableWriter
--runNulls Nothing l r  = modify'
--    (\s -> s { currentRun = (currentRun s) { nullRunLen = (l + 1) }})
--runNulls (Just c) l r = putFrame >> modify'
--    (\s -> s { currentRun = ValueRun 1 (r + l) c })
--
--runUniques :: Maybe Cell -> Word64 -> Word64 -> DiffList Cell -> Cell -> LazyTableWriter
--runUniques Nothing l r _ _ = putFrame >> modify'
--    (\s -> s { currentRun = NullRun 1 (r + l) })
--runUniques (Just c) l r vs v
--    -- turn it into a value run if we see two in a row
--    | c == v = modify'
--        -- pull the last value off of the unique run
--        (\s -> s {
--            currentRun = (currentRun s) {
--                uniqueRunVals = tail . vs
--              , uniqueRunLen  = l - 1
--              }
--          }
--        )
--        -- output the unique run
--        >> putFrame
--        -- create a new value run with our new cells
--        >> modify' (\s -> s { currentRun = ValueRun 2 (r + l - 1) c })
--    -- keep going with the current unique run
--    | otherwise = modify'
--        (\s -> s {
--            currentRun = (currentRun s) {
--                uniqueRunLen  = l + 1
--              , uniqueRunVals = vs . (c:)
--              , uniqueRunLast = c
--              }
--          }
--        )
--
--runValues :: Maybe Cell -> Word64 -> Word64 -> Cell -> LazyTableWriter
--runValues Nothing l r _ = putFrame >> modify'
--    (\s -> s { currentRun = NullRun 1 (r + l)})
--runValues (Just c) l r v
--    -- keep our current value run if the next value is the same as our runs
--    | c == v = modify'
--        (\s -> s { currentRun = (currentRun s) { valueRunLen = l + 1 } })
--    -- we just saw a different value
--    -- if this is a singleton value run, convert it into a unique run
--    | l == 1 = modify' (\s -> s { currentRun = UniqueRun 2 r ((v:) . (c:)) c })
--    -- output our current value run and make a new singleton value run
--    | otherwise = putFrame >> modify'
--        (\s -> s { currentRun = ValueRun 1 (r + l) c })
--
--putFrame :: LazyTableWriter
--putFrame = do
--    run <- gets currentRun
--    case run of
--        NullRun l r        -> do
--            putInStateFP columnFP $ putWord16le 0
--            putInStateFP columnFP $ putWord64le r
--            putInStateFP columnFP $ putWord64le l
--        UniqueRun l r vs v -> do
--            putInStateFP columnFP $ putWord16le $ fromIntegral l .|. shift 0x02 14
--            putInStateFP columnFP $ putWord64le r
--            putCell (CellArray (V.fromList $ vs []))
--        ValueRun l r v     -> do
--            putInStateFP columnFP $ putWord16le $ fromIntegral l .|. shift 0x03 14
--            putInStateFP columnFP $ putWord64le r
--            putCell v
--        UninitializedRun   -> error "Uninitialized run"
--
---- | Write a cell to the current column and literal tables if necessary.
--putCell :: Cell -> LazyTableWriter
--putCell c = do
--    storeCell c
--    s <- get
--    putInStateFP columnFP $ putCellValue s c
--    where
--        -- | Put inline cell value.
--        putCellValue :: LazyTableState -> Cell -> Put
--        putCellValue s (CellUInt i)     = putWord64le i
--        putCellValue s (CellInt i)      = putWord64le $ fromIntegral i
--        putCellValue s (CellDouble d)   = unsafePutIEEEDouble d
--        putCellValue s (CellDateTime t) =
--            putWord64le $ fromInteger $ floor $ utcTimeToPOSIXSeconds t
--        putCellValue s (CellString t)   =
--            -- it's valid to use fromJust because the storeCell call will always
--            -- insert this value into the map
--            putWord64le $ fromJust $ M.lookup t $ stringLitCache s
--        putCellValue s (CellBinary b)   =
--            putWord64le $ fromJust $ M.lookup b $ binaryLitCache s
--        putCellValue s (CellArray a)    =
--            putWord64le $ fromJust $ M.lookup a $ arrayLitCache s
--
--        -- | Write cell to associated literal table file and literal cache.
--        storeCell :: Cell -> LazyTableWriter
--        storeCell (CellUInt i)     = return ()
--        storeCell (CellInt i)      = return ()
--        storeCell (CellDouble d)   = return ()
--        storeCell (CellDateTime t) = return ()
--        storeCell (CellString t)   =
--            storeVarCell
--                t
--                stringLitFP
--                stringLitCache
--                (\s a -> s { stringLitCache = a })
--                (putTextUtf8 t)
--        storeCell (CellBinary b)   =
--            storeVarCell
--                b
--                binaryLitFP
--                binaryLitCache
--                (\s a -> s { binaryLitCache = a })
--                (putSizedByteString b)
--        storeCell (CellArray a)    = do
--            V.mapM_ storeCell a
--            s <- get
--            storeVarCell
--                a
--                arrayLitFP
--                arrayLitCache
--                (\s a -> s { arrayLitCache = a })
--                (putWord64le (fromIntegral $ V.length a) >> V.mapM_ (putCellValue s) a)
--
--        storeVarCell :: (Ord k) => k
--                     -> (LazyTableState -> (FilePath, Handle))
--                     -> (LazyTableState -> M.Map k Word64)
--                     -> (LazyTableState -> M.Map k Word64 -> LazyTableState)
--                     -> Put
--                     -> LazyTableWriter
--        storeVarCell k fp getCache setCache p = do
--            cache <- gets getCache
--            offset <- gets fp >>= fmap fromIntegral . liftIO . hTell . snd
--            modify' $ flip setCache (M.insert k offset cache)
--            putInStateFP fp p
--
--putInStateFP :: (LazyTableState -> (FilePath, Handle)) -> Put -> LazyTableWriter
--putInStateFP accessor p = do
--    (_, h) <- gets accessor
--    liftIO $ putHandle h p
--
--
---- | This function uses memory allocation that comes back in the IO monad
---- but since it is only doing memory allocation, we can use unsafeLocalState
---- to get it back into a usable context for Putting.
---- It exists because we need to be able to write the double as the 8 bytes
---- it actually is with GHC, but we don't have the ability to access that without
---- turning it into a word64
--unsafePutIEEEDouble :: Double -> Put
--unsafePutIEEEDouble = putWord64le . unsafeLocalState . unsafeDtoW64
--    where unsafeDtoW64 d = alloca (\p -> poke p d >> peek (castPtr p))
