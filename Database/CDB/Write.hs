module Database.CDB.Write (
  CDBMake(),
  cdbMake,
  cdbAdd,
  cdbAddMany
) where

import Control.Applicative
import Control.Monad
import Control.Monad.State
import Data.Array.IO
import Data.Array.Unboxed
import qualified Data.ByteString as ByteString
import Data.ByteString (ByteString)
import Data.IORef
import Data.List
import Data.Maybe
import Data.Word
import Database.CDB.Packable
import Database.CDB.Util
import System.Directory
import System.FilePath
import System.IO

-- |Construct a CDB as described inside the supplied CDBMake computation.
--  During construction, it will be written to a temporary file and then
--  moved over top of the given file atomically.
cdbMake :: FilePath -> CDBMake -> IO ()
cdbMake fileName f = do
  let tmp = fileName <.> "tmp"
  h <- openBinaryFile tmp WriteMode
  hSeek h AbsoluteSeek (256*8)
  initState <- initialMakeState h
  cdb <- execStateT f initState
  tablesArrays <- unsafeFreeze (cdbstTables cdb) :: IO (Array Word8 [CDBSlot])
  let tables = elems tablesArrays
  writeHashTables h tables
  hClose h
  renameFile tmp fileName

-- |Adds a given key-value pair to the CDB being built.
cdbAdd :: (Packable k, Packable v) => k -> v -> CDBMake
cdbAdd k v = do
  let (pk, pv) = (pack k, pack v)
  cdbAddSlot pk pv
  cdbWriteRecord pk pv

-- |Add a list of key-value pairs to the CDB being built.
cdbAddMany :: (Packable k, Packable v) => [(k,v)] -> CDBMake
cdbAddMany = mapM_ (uncurry cdbAdd)

-----------------------
-- write implementation
-----------------------

type CDBMake = StateT CDBMakeState IO ()

data CDBMakeState = CDBMakeState {
  cdbstHandle        :: Handle,
  cdbstRecordsEnd    :: Word32,
  cdbstTables        :: IOArray Word8 [CDBSlot] 
}

type CDBSlot = (Word32, Word32)

initialMakeState :: Handle -> IO CDBMakeState
initialMakeState h = do
  tables <- newArray (0, 255) []
  return CDBMakeState {
    cdbstTables     = tables,
    cdbstRecordsEnd = 256*8,
    cdbstHandle     = h
  }

-- add a slot to the set of slots
cdbAddSlot :: ByteString -> ByteString -> CDBMake
cdbAddSlot k v = do
  let hash     = cdbHash k
  let tableNum = fromIntegral $ hash `mod` 256
  cdb <- get
  let pointer = cdbstRecordsEnd cdb
  let tables = cdbstTables cdb
  oldTable <- liftIO $ readArray tables tableNum
  liftIO $ writeArray tables tableNum $ (hash, pointer):oldTable


cdbWriteRecord :: ByteString -> ByteString -> CDBMake
cdbWriteRecord k v = 
  let lk  = fromIntegral $ ByteString.length k
      lv  = fromIntegral $ ByteString.length v
      record = ByteString.concat [pack lk, pack lv, k, v]
  in do
    cdb <- get
    liftIO $ ByteString.hPut (cdbstHandle cdb) record
    put $ cdb { cdbstRecordsEnd = cdbstRecordsEnd cdb + lk + lv + 8 }

-- assumes the Handle is pointing to right after the last record written
writeHashTables :: Handle -> [[CDBSlot]] -> IO ()
writeHashTables h tables = do
  tableBase <- fromIntegral <$> hTell h
  let bufSize = fromIntegral $ (*4)  $ sum (map length tables)
  buf <- newArray (0, bufSize-1) 0 
  bufOffset <- newIORef 0
  pointers <- mapM (writeTable buf bufOffset tableBase) tables
  ibuf <- unsafeFreeze buf :: IO (UArray Word32 Word32)
  ByteString.hPut h (pack ibuf)
  writePointers h pointers

writeTable :: IOUArray Word32 Word32 ->
              IORef Word32 ->
              Word32 ->
              [CDBSlot] ->
              IO (Word32, Word32)
writeTable buf bufOffset tableBase table = do
  -- compute the number of slots
  -- twice the number of actual entries to help prevent collision
  let tableLength = length table * 2
  pointer <- readIORef bufOffset 
  -- write the slots in the order they came in
  mapM_ (writeSlot buf pointer tableLength) (reverse table)
  writeIORef bufOffset $ pointer + fromIntegral tableLength * 2
  return (pointer * 4 + tableBase, fromIntegral tableLength)

writeSlot :: IOUArray Word32 Word32 -> Word32 -> Int -> CDBSlot -> IO ()
writeSlot buf bufOffset tableLength (hash, pointer) = do
  ibuf <- unsafeFreeze buf
  let slot = findEmptySlot ibuf bufOffset tableLength hash
  writeArray buf slot hash
  writeArray buf (slot+1) pointer

findEmptySlot :: UArray Word32 Word32 -> Word32 -> Int -> Word32 -> Word32
findEmptySlot buf bufOffset tl hash =
  let tl' = fromIntegral tl
      searchStart = (hash `div` 256 `mod` tl') * 2
      linearSearch i = if buf ! (bufOffset+i+1) == 0
                         then bufOffset + i
                         else linearSearch $ (i + 2) `mod` (tl' * 2)
  in
  linearSearch searchStart

writePointers :: Handle -> [(Word32, Word32)] -> IO ()
writePointers h pointers = do
  hSeek h AbsoluteSeek 0
  mapM_ (\(pointer, tableLength) -> do ByteString.hPut h (pack pointer)
                                       ByteString.hPut h (pack tableLength))
        pointers

