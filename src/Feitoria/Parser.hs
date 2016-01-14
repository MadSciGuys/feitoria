{-# LANGUAGE OverloadedStrings
           , BangPatterns
           #-}

module Feitoria.Parser where

import Data.ByteString.Lazy as B

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
import Foreign.Storable
import Foreign.Marshall.Alloc
import Foreign.Marshall.Unsafe (unsafeLocalState)

import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import qualified Data.Vector as V

import Feitoria.Types

import System.Directory
import System.IO.Temp

type LazyTableWriter = StateT LazyTableState IO ()

type DiffList a = [a] -> [a]

data LazyTableState = LazyTableState {
    stringLitCache :: M.Map T.Text Int
  , arrayLitCache  :: M.Map (V.Vector Cell) Int
  , binaryLitCache :: M.Map B.ByteString Int
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
                      nullRunLen :: !Int
                    , nullRunRow :: !Int
                    }
                  | UniqueRun {
                      uniqueRunLen  :: !Int
                    , uniqueRunRow  :: !Int
                    , uniqueRunVals :: DiffList Cell
                    , uniqueRunLast :: !Cell
                    }
                  | ValueRun {
                      valueRunLen :: !Int
                    , valueRunRow :: !Int
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
    outfile     <- openFile o WriteMode
    putHandle outfile $ putTableHeader (lazyTblHeader o)
    mapM_ (putColumnCells . lazyCells) (lazyTblCols t)
    offsets     <- ($ []) <$> gets columnOffsets
    let colheaders = zipWith MMapColumn (map lazyHeader (lazyTblCols t)) offsets
    mapM_ putColumnHeader colheaders
    -- get all the temporary files
    (chfp, chh) <- gets colHeaderFP
    (ctfp, cth) <- gets columnFP
    (stfp, sth) <- gets stringLitFP
    (atfp, ath) <- gets arrayLitFP
    (btfp, bth) <- gets binaryLitFP
    -- get the size of all of the temporary files
    th_len      <- fromIntegral <$> hTell outfile
    ch_len      <- fromIntegral <$> hTell chh
    ct_len      <- fromIntegral <$> hTell cth
    st_len      <- fromIntegral <$> hTell sth
    at_len      <- fromIntegral <$> hTell ath
    bt_len      <- fromIntegral <$> hTell bth
    col_count   <- gets colCount
    rec_count   <- fromMaybe 0 <$> gets validColLen
    -- get the offsets for the various tables
    let -- 4 offset uint64s + 2 col/row count uint64s + 4 magic bytes = 52
        ct_offset = th_len + ch_len + 52
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
                closeTempFP chh
                catHandles outfile cth
                closeTempFP cth
                catHandles outfile sth
                closeTempFP sth
                catHandles outfile ath
                closeTempFP ath
                catHandles outfile bth
                closeTempFP bth
                hClose outfile

-- | Run a 'Put' and write the output to a 'Handle' in one go. Assumes the 'Put'
--   is terminal.
putHandle :: Handle -> Put -> IO ()
putHandle h p = runPut p >>= B.hPut h

-- | Assumes the source handle is finite. Please make this exception safe.
catHandles :: Handle -> Handle -> IO ()
catHandles sink source = B.hGetContents source >>= B.hPut sink

putTableHeader :: TableHeader -> Put
putTableHeader t = do
    putByteString "w\01\01\01"
    putWord64le (tblProtVersion t)
    putStrictByteString (T.encodeUtf8 (tblTitle t))

putColumnHeader :: MMapColumn -> Put
putColumnHeader (MMapColumn c o) = do
    putStrictByteString (T.encodeUtf8 (colName c))
    putCellType (colType c)
    putWord64le (colArrayDepth c)
    putWord64le (fromIntegral o)

-- | This just writes the frames in linear order right now.
putColumnCells :: [Maybe Cell] -> LazyTableWriter
putColumnCells [] = finalizeCol
putColumnCells cs = do
    offset <- fromIntegral <$> (gets (snd . columnFP) >>= liftIO hTell)
    modify' (\s -> s { columnOffsets = (columnOffsets s) . (offset:) })
    mapM_ runFrames cs
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

runNulls :: Maybe Cell -> Int -> Int -> LazyTableWriter
runNulls Nothing l r  = modify'
    (\s -> s { currentRun = (currentRun s) { nullRunLen = (l + 1) }})
runNulls (Just c) l r = putFrame >> modify'
    (\s -> s { currentRun = ValueRun 1 (r + l) c })

runUniques :: Maybe Cell -> Int -> Int -> DiffList Cell -> Cell -> LazyTableWriter
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
    | l == 1 = modify' (\s -> s { currentRun = UniqueRun 2 r ((v:) . (c:)) c })
    -- output our current value run and make a new singleton value run
    | otherwise = putFrame >> modify'
        (\s -> s { currentRun = ValueRun 1 (r + l) c })

putFrame :: LazyTableWriter
putFrame = undefined

putCell :: Cell -> LazyTableWriter
putCell (CellUInt i)     = lift $ gets putHandle (putWord64le i)
putCell (CellInt i)      = lift $ putWord64le (fromIntegral i)
putCell (CellDouble d)   = lift $ unsafePutIEEEDouble d
putCell (CellDateTime t) = lift $ putWord64le $ fromIntegral $ utcTimeToPOSIXSeconds t
putCell (CellString t)   = 
putCell (CellBinary b)   =
putCell (CellBoolean b)  =
putCell (CellArray a)    =

putInColumnFP :: Put -> LazyTableWriter

putCellType :: CellType -> Put
putCellType TypeUInt          = putWord8 0
putCellType TypeInt           = putWord8 1
putCellType TypeDouble        = putWord8 2
putCellType TypeDateTime      = putWord8 3
putCellType TypeString        = putWord8 4
putCellType (TypeBinary mime) = putWord8 5
    >> putStrictByteString (T.encodeUtf8 ((showMIMEType mime))
putCellType TypeBoolean       = putWord8 6

putStrictByteString :: B.ByteString -> Put
putStrictByteString = (putWord8 0 <*) . putByteString

-- | This function uses memory allocation that comes back in the IO monad
-- but since it is only doing memory allocation, we can use unsafeLocalState
-- to get it back into a usable context for Putting.
-- It exists because we need to be able to write the double as the 8 bytes
-- it actually is with GHC, but we don't have the ability to access that without
-- turning it into a word64
unsafePutIEEEDouble :: Double -> Put
unsafePutIEEEDouble = putWord64le . unsafeLocalState . unsafeDtoW64
    where unsafeDtoW64 d = alloca (\p -> poke p d >> peek (castPtr p))
