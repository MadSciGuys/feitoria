{-# LANGUAGE OverloadedStrings
           , BangPatterns
           #-}

module Feitoria.Writer where

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

type LazyTableWriter = StateT LazyTableState IO ()

type DiffList a = [a] -> [a]

data LazyTableState = LazyTableState {
    stringLitCache :: M.Map T.Text Word64
  , arrayLitCache  :: M.Map (V.Vector Cell) Word64
  , binaryLitCache :: M.Map B.ByteString Word64
  , currentRun     :: !LazyTableRun
  , currentColLen  :: !Word64
  , validColLen    :: !(Maybe Word64)
  , colCount       :: !Word64
  , colHeaderFP    :: (FilePath, Handle)
  , columnFP       :: (FilePath, Handle)
  , stringLitFP    :: (FilePath, Handle)
  , arrayLitFP     :: (FilePath, Handle)
  , binaryLitFP    :: (FilePath, Handle)
  , columnOffsets  :: DiffList Int
  }

data LazyTableRun = NullRun {
                      nullRunLen :: !Word64
                    , nullRunRow :: !Word64
                    }
                  | UniqueRun {
                      uniqueRunLen  :: !Word64
                    , uniqueRunRow  :: !Word64
                    , uniqueRunVals :: DiffList Cell
                    , uniqueRunLast :: !Cell
                    }
                  | ValueRun {
                      valueRunLen :: !Word64
                    , valueRunRow :: !Word64
                    , valueRunVal :: !Cell
                    }
                  | UninitializedRun

putTable :: LazyTable -- ^ The table to write to disk.
         -> FilePath  -- ^ A directory in which temporary files may be created.
         -> FilePath  -- ^ File name to which the table will be written.
         -> IO ()
putTable table tempdir outfile = initState
                             >>= void . runStateT (putTable' table outfile)
    -- Please make this even more exception safe.
    where initState = bracketOnError acqFPs (mapM_ closeTempFP) mkInitState
          acqFPs = replicateM 5 (openBinaryTempFile tempdir tnf)
          tnf = "libfeit"
          mkInitState [ch,ct,st,at,bt] = return $
                    LazyTableState M.empty
                                   M.empty
                                   M.empty
                                   UninitializedRun
                                   0
                                   Nothing
                                   0
                                   ch
                                   ct
                                   st
                                   at
                                   bt
                                   id

-- | Close a temp file's 'Handle' and delete the file.
closeTempFP :: (FilePath, Handle) -> IO ()
closeTempFP (f, h) = hClose h >> removeFile f

-- | If it is possible for putCells to fail silently somehow, we need to check
--   the length of the list of headers and compare it to the length of the list
--   of cell offsets.
putTable' :: LazyTable -> FilePath -> LazyTableWriter
putTable' t o = do
    outfile     <- liftIO $ openFile o WriteMode
    liftIO $ putHandle outfile $ putTableHeader (lazyTblHeader t)
    mapM_ (putColumnCells . lazyCells) (lazyTblCols t)
    offsets     <- ($ []) <$> gets columnOffsets
    let colheaders = zipWith MMapColumn (map lazyHeader (lazyTblCols t)) offsets
    mapM_ (putInStateFP colHeaderFP . putColumnHeader) colheaders
    -- get all the temporary files
    ch@(chfp, chh) <- gets colHeaderFP
    ct@(ctfp, cth) <- gets columnFP
    st@(stfp, sth) <- gets stringLitFP
    at@(atfp, ath) <- gets arrayLitFP
    bt@(btfp, bth) <- gets binaryLitFP
    -- get the size of all of the temporary files
    th_len <- liftIO $ fromIntegral <$> hTell outfile
    ch_len <- liftIO $ fromIntegral <$> hTell chh
    ct_len <- liftIO $ fromIntegral <$> hTell cth
    st_len <- liftIO $ fromIntegral <$> hTell sth
    at_len <- liftIO $ fromIntegral <$> hTell ath
    bt_len <- liftIO $ fromIntegral <$> hTell bth
    liftIO $ do hSeek chh AbsoluteSeek 0
                hSeek cth AbsoluteSeek 0
                hSeek sth AbsoluteSeek 0
                hSeek ath AbsoluteSeek 0
                hSeek bth AbsoluteSeek 0
    col_count   <- gets colCount
    rec_count   <- fromMaybe 0 <$> gets validColLen
    -- get the offsets for the various tables
    let -- 4 offset uint64s + 2 col/row count uint64s = 48
        ct_offset = th_len + ch_len + 48
        st_offset = ct_offset + ct_len
        at_offset = st_offset + st_len
        bt_offset = at_offset + at_len
    -- Here's where we actually build the final output file:
    liftIO $ do putHandle outfile $ putWord64le col_count
                putHandle outfile $ putWord64le rec_count
                putHandle outfile $ putWord64le ct_offset
                putHandle outfile $ putWord64le st_offset
                putHandle outfile $ putWord64le at_offset
                putHandle outfile $ putWord64le bt_offset
                catHandles outfile chh
                closeTempFP ch
                catHandles outfile cth
                closeTempFP ct
                catHandles outfile sth
                closeTempFP st
                catHandles outfile ath
                closeTempFP at
                catHandles outfile bth
                closeTempFP bt
                hClose outfile

-- | Run a 'Put' and write the output to a 'Handle' in one go. Assumes the 'Put'
--   is terminal.
putHandle :: Handle -> Put -> IO ()
putHandle h = BL.hPut h . runPut

-- | Assumes the source handle is finite. Please make this exception safe.
catHandles :: Handle -> Handle -> IO ()
catHandles sink source = B.hGetContents source >>= B.hPut sink

putTableHeader :: TableHeader -> Put
putTableHeader t = do
    putByteString "w\01\01\01"
    putWord64le (tblProtVersion t)
    putTextUtf8 (tblTitle t)

putColumnHeader :: MMapColumn -> Put
putColumnHeader (MMapColumn c o) = do
    putTextUtf8 (colName c)
    putCellType (colType c)
    putWord64le (colArrayDepth c)
    putWord64le (fromIntegral o)

-- | This just writes the frames in linear order right now.
putColumnCells :: [Maybe Cell] -> LazyTableWriter
putColumnCells [] = finalizeCol
putColumnCells cs = do
    offset <- fromIntegral <$> (gets (snd . columnFP) >>= liftIO . hTell)
    modify' (\s -> s { columnOffsets = (columnOffsets s) . (offset:) })
    mapM_ runFrames cs
    putFrame
    finalizeCol

-- | Get ready to write the next column. Find a better way to throw an error in
--   this monad stack.
finalizeCol :: LazyTableWriter
finalizeCol = do
    vcl <- gets validColLen
    ccl <- gets currentColLen
    let vcl' = maybe (Just ccl)
                     (\v -> if v == ccl then Just v
                            else error "columns must have same length")
                     vcl
    modify' (\s -> s { currentRun = UninitializedRun
                     , currentColLen = 0
                     , validColLen = vcl'
                     , colCount = (colCount s) + 1
                     })

runFrames :: Maybe Cell -> LazyTableWriter
runFrames c = do
    modify' (\s -> s { currentColLen = (currentColLen s) + 1 })
    cr <- gets currentRun
    case cr of (NullRun l r)        -> runNulls c l r
               (UniqueRun l r vs v) -> runUniques c l r vs v
               (ValueRun l r v)     -> runValues c l r v
               UninitializedRun     -> firstRun c

firstRun :: Maybe Cell -> LazyTableWriter
firstRun Nothing  = modify' (\s -> s { currentRun = NullRun 1 0 })
firstRun (Just c) = modify' (\s -> s { currentRun = ValueRun 1 0 c})

runNulls :: Maybe Cell -> Word64 -> Word64 -> LazyTableWriter
runNulls Nothing l r  = modify'
    (\s -> s { currentRun = (currentRun s) { nullRunLen = (l + 1) }})
runNulls (Just c) l r = putFrame >> modify'
    (\s -> s { currentRun = ValueRun 1 (r + l) c })

runUniques :: Maybe Cell -> Word64 -> Word64 -> DiffList Cell -> Cell -> LazyTableWriter
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

runValues :: Maybe Cell -> Word64 -> Word64 -> Cell -> LazyTableWriter
runValues Nothing l r _ = putFrame >> modify'
    (\s -> s { currentRun = NullRun 1 (r + l)})
runValues (Just c) l r v
    -- keep our current value run if the next value is the same as our runs
    | c == v = modify'
        (\s -> s { currentRun = (currentRun s) { valueRunLen = l + 1 } })
    -- we just saw a different value
    -- if this is a singleton value run, convert it into a unique run
    | l == 1 = modify' (\s -> s { currentRun = UniqueRun 2 r ((v:) . (c:)) c })
    -- output our current value run and make a new singleton value run
    | otherwise = putFrame >> modify'
        (\s -> s { currentRun = ValueRun 1 (r + l) c })

putFrame :: LazyTableWriter
putFrame = do
    run <- gets currentRun
    case run of
        NullRun l r        -> do
            putInStateFP columnFP $ putWord16le 0
            putInStateFP columnFP $ putWord64le r
            putInStateFP columnFP $ putWord64le l
        UniqueRun l r vs v -> do
            putInStateFP columnFP $ putWord16le $ fromIntegral l .|. shift 0x02 14
            putInStateFP columnFP $ putWord64le r
            putCell (CellArray (V.fromList $ vs []))
        ValueRun l r v     -> do
            putInStateFP columnFP $ putWord16le $ fromIntegral l .|. shift 0x03 14
            putInStateFP columnFP $ putWord64le r
            putCell v
        UninitializedRun   -> error "Uninitialized run"

-- | Write a cell to the current column and literal tables if necessary.
putCell :: Cell -> LazyTableWriter
putCell c = do
    storeCell c
    s <- get
    putInStateFP columnFP $ putCellValue s c
    where
        -- | Put inline cell value.
        putCellValue :: LazyTableState -> Cell -> Put
        putCellValue s (CellUInt i)     = putWord64le i
        putCellValue s (CellInt i)      = putWord64le $ fromIntegral i
        putCellValue s (CellDouble d)   = unsafePutIEEEDouble d
        putCellValue s (CellDateTime t) =
            putWord64le $ fromInteger $ floor $ utcTimeToPOSIXSeconds t
        putCellValue s (CellString t)   =
            -- it's valid to use fromJust because the storeCell call will always
            -- insert this value into the map
            putWord64le $ fromJust $ M.lookup t $ stringLitCache s
        putCellValue s (CellBinary b)   =
            putWord64le $ fromJust $ M.lookup b $ binaryLitCache s
        putCellValue s (CellArray a)    =
            putWord64le $ fromJust $ M.lookup a $ arrayLitCache s

        -- | Write cell to associated literal table file and literal cache.
        storeCell :: Cell -> LazyTableWriter
        storeCell (CellUInt i)     = return ()
        storeCell (CellInt i)      = return ()
        storeCell (CellDouble d)   = return ()
        storeCell (CellDateTime t) = return ()
        storeCell (CellString t)   =
            storeVarCell
                t
                stringLitFP
                stringLitCache
                (\s a -> s { stringLitCache = a })
                (putTextUtf8 t)
        storeCell (CellBinary b)   =
            storeVarCell
                b
                binaryLitFP
                binaryLitCache
                (\s a -> s { binaryLitCache = a })
                (putSizedByteString b)
        storeCell (CellArray a)    = do
            V.mapM_ storeCell a
            s <- get
            storeVarCell
                a
                arrayLitFP
                arrayLitCache
                (\s a -> s { arrayLitCache = a })
                (putWord64le (fromIntegral $ V.length a) >> V.mapM_ (putCellValue s) a)

        storeVarCell :: (Ord k) => k
                     -> (LazyTableState -> (FilePath, Handle))
                     -> (LazyTableState -> M.Map k Word64)
                     -> (LazyTableState -> M.Map k Word64 -> LazyTableState)
                     -> Put
                     -> LazyTableWriter
        storeVarCell k fp getCache setCache p = do
            cache <- gets getCache
            offset <- gets fp >>= fmap fromIntegral . liftIO . hTell . snd
            modify' $ flip setCache (M.insert k offset cache)
            putInStateFP fp p

putInStateFP :: (LazyTableState -> (FilePath, Handle)) -> Put -> LazyTableWriter
putInStateFP accessor p = do
    (_, h) <- gets accessor
    liftIO $ putHandle h p

putCellType :: CellType -> Put
putCellType TypeUInt          = putWord8 0
putCellType TypeInt           = putWord8 1
putCellType TypeDouble        = putWord8 2
putCellType TypeDateTime      = putWord8 3
putCellType TypeString        = putWord8 4
putCellType (TypeBinary mime) = putWord8 5
    >> putTextUtf8 (showMIMEType mime)
putCellType TypeBoolean       = putWord8 6

putNullByteString :: B.ByteString -> Put
putNullByteString = (*> putWord8 0) . putByteString

putTextUtf8 :: T.Text -> Put
putTextUtf8 = putNullByteString . T.encodeUtf8

putSizedByteString :: B.ByteString -> Put
putSizedByteString b = do
    putWord64le $ fromIntegral $ B.length b
    putByteString b

-- | This function uses memory allocation that comes back in the IO monad
-- but since it is only doing memory allocation, we can use unsafeLocalState
-- to get it back into a usable context for Putting.
-- It exists because we need to be able to write the double as the 8 bytes
-- it actually is with GHC, but we don't have the ability to access that without
-- turning it into a word64
unsafePutIEEEDouble :: Double -> Put
unsafePutIEEEDouble = putWord64le . unsafeLocalState . unsafeDtoW64
    where unsafeDtoW64 d = alloca (\p -> poke p d >> peek (castPtr p))
