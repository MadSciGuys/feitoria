module Feitoria.IndexedBuilder where

import           Control.Monad.Trans

import qualified Data.ByteString         as B
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy    as L

import           Data.Int

import           Data.Monoid

import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T

import           Data.Word

import           System.IO

-- | A 'B.Builder' that knows how many bytes it will contribute.
data IndexedBuilder = IndexedBuilder {
    ibBuilder :: B.Builder
  , ibIndex   :: !Integer
  }

instance Monoid IndexedBuilder where
    mempty  = IndexedBuilder mempty 0
    mappend (IndexedBuilder b i) (IndexedBuilder b' i') = IndexedBuilder (b <> b') (i + i')

byteString :: B.ByteString -> IndexedBuilder
byteString b = IndexedBuilder (B.byteString b) (fromIntegral (B.length b))

-- | Don't use this; it forces the whole lazy 'L.ByteString'.
lazyByteString :: L.ByteString -> IndexedBuilder
lazyByteString b = IndexedBuilder (B.lazyByteString b) (fromIntegral (L.length b))

int8 :: Int8 -> IndexedBuilder
int8 i = IndexedBuilder (B.int8 i) 1

word8 :: Word8 -> IndexedBuilder
word8 w = IndexedBuilder (B.word8 w) 1

int16LE :: Int16 -> IndexedBuilder
int16LE i = IndexedBuilder (B.int16LE i) 2

word16LE :: Word16 -> IndexedBuilder
word16LE w = IndexedBuilder (B.word16LE w) 2

int32LE :: Int32 -> IndexedBuilder
int32LE i = IndexedBuilder (B.int32LE i) 4

word32LE :: Word32 -> IndexedBuilder
word32LE w = IndexedBuilder (B.word32LE w) 4

int64LE :: Int64 -> IndexedBuilder
int64LE i = IndexedBuilder (B.int64LE i) 8

word64LE :: Word64 -> IndexedBuilder
word64LE w = IndexedBuilder (B.word64LE w) 8

floatLE :: Float -> IndexedBuilder
floatLE f = IndexedBuilder (B.floatLE f) 4

doubleLE :: Double -> IndexedBuilder
doubleLE d = IndexedBuilder (B.doubleLE d) 8

-- | Serialize a 'B.ByteString' C-style, i.e. with a null terminator.
byteStringCString :: B.ByteString -> IndexedBuilder
byteStringCString b = byteString b <> word8 0

-- | Serialize a 'B.ByteString' Pascal-style, i.e. with a preceding 64-bit word
--   storing the length.
byteStringPascalString :: B.ByteString -> IndexedBuilder
byteStringPascalString b = word64LE (fromIntegral (B.length b))
                        <> byteString b

-- | Encode a 'T.Text' in UTF8 and serialize it C-style.
textUtf8 :: T.Text -> IndexedBuilder
textUtf8 = byteStringCString . T.encodeUtf8

data PosHandle = PosHandle {
    phHandle :: Handle
  , phPos    :: !Integer
  }

-- | Check that a 'PosHandle' position is accurate.
hCheckPos :: MonadIO m => PosHandle -> m Bool
hCheckPos (PosHandle h p) = (== p) <$> (liftIO (hTell h))

hPutIndexedBuilderM :: MonadIO m => PosHandle -> IndexedBuilder -> m PosHandle
hPutIndexedBuilderM (PosHandle h p) (IndexedBuilder b i) = liftIO
    (B.hPutBuilder h b >> return (PosHandle h (p+i)))
