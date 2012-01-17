-- |
-- Module      : Database.CDB
-- Copyright   : (c) Adam Smith 2012
--
-- License     : BSD-style
--
-- A library for reading and writing CDB (Constant Database) files.
--
-- CDB files are immutable key-value stores, designed for extremely fast and
-- memory-efficient construction and lookup. They can be as large as 4GB, and
-- at no point in their construction or use must all data be loaded into
-- memory. CDB files can contain multiple values for a given key.
--
-- For more information on the CDB file format, please see:
--     <http://cr.yp.to/cdb.html>
--
-- Using @hs-cdb@ should be fairly straightforward. Here's a simple example:
-- 
-- >    printStuff :: IO ()
-- >    printStuff = do
-- >      cdb <- cdbInit "my.cdb"
-- >      let foo = cdbGet cdb "foo"
-- >      let bars = cdbGetAll cdb "bar"
-- >      maybe (putStrLn "Not found") putStrLn foo
-- >      mapM_ putStrLn bars
--
-- The CDB will be automatically cleaned up by the garbage collector after use.
--
-- The only sticking point may be the use of the 'Packable' and 'Unpackable'
-- classes. This allows the @hs-cdb@ interface to be both generic (so your CDB
-- can store effectively any kind of data) but also convenient in the common
-- case of plaintext data. Internally, @hs-cdb@ uses 'ByteString's, but it will
-- automatically pack and unpack keys and values to suit the types you're using
-- in your program. In particular, in an instance is provided for 'String', so
-- @hs-cdb@ can use 'String's as keys and values transparently.
--
-- Writing a CDB is just as straightforward:
--
-- >    makeCDB :: IO ()
-- >    makeCDB = cdbMake "my.cdb" $ do
-- >      cdbAdd "foo" "this is the data associated with foo"
-- >      cdbAddMany [("bar1", "bar1data"), ("bar2", "bar2data")]
--
-- Again, @hs-cdb@ automatically closes the files after use. Moreover, in CDB
-- tradition, @hs-cdb@ actually creates a CDB named @file.cdb@ by first writing 
-- it to @file.cdb.tmp@, and then atomically renaming it over @file.cdb@. This
-- means that readers never need to pause when you're regenerating a CDB.

{-# LANGUAGE FlexibleInstances, TypeSynonymInstances #-}

module Database.CDB (
  -- * The @CDB@ type
  CDB(),
  -- * Classes
  Packable,
  Unpackable,
  -- * Reading interface
  cdbInit,
  cdbGet,
  cdbGetAll,
  cdbHasKey,
  cdbCount,
  -- * Writing interface
  cdbMake,
  cdbAdd,
  cdbAddMany) where

import Control.Applicative
import Control.Monad
import Control.Monad.State
import Data.Array.IO
import Data.Array.Unboxed
import Data.Bits
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as ByteString.Char8
import Data.ByteString (ByteString)
import Data.IORef
import Data.List
import Data.Maybe
import Data.Word
import System.Directory
import System.FilePath
import System.IO
import System.IO.Posix.MMap

------------
-- interface
------------

-- |Internal representation of a CDB file on disk.
data CDB = CDB { cdbMem :: ByteString }

-- |An instance of 'Packable' can be losslessly transformed into a 'ByteString'.
class Packable k where
  pack :: k -> ByteString

-- |An instance of 'Unpackable' can be losslessly transformed from a 'ByteString'.
class Unpackable v where
  unpack :: ByteString -> v

instance Packable ByteString where
  pack = id

instance Unpackable ByteString where
  unpack = id

instance Packable [Char] where
  pack = ByteString.Char8.pack

instance Unpackable [Char] where
  unpack = ByteString.Char8.unpack

instance Packable [Word8] where
  pack = ByteString.pack

instance Unpackable [Word8] where
  unpack = ByteString.unpack

instance Packable Word32 where
  pack n = ByteString.pack $ map fromIntegral 
    [n .&. 0xFF,
    (n `shiftR` 8) .&. 0xFF,
    (n `shiftR` 16) .&. 0xFF,
    (n `shiftR` 24) .&. 0xFF]

instance Packable (Array Word32 Word32) where
  pack t = pack $ ByteString.concat $ map pack $ elems t

-- |Loads a CDB from a file.
cdbInit :: FilePath -> IO CDB
cdbInit f = liftM CDB $ unsafeMMapFile f 

-- |Finds the first entry associated with a key in a CDB.
cdbGet :: (Packable k, Unpackable v) => CDB -> k -> Maybe v
cdbGet cdb key = case cdbFind cdb (pack key) of
  []    -> Nothing
  (x:_) -> return $ unpack $ readData cdb x 

-- |Finds all entries associated with a key in a CDB.
cdbGetAll :: (Packable k, Unpackable v) => CDB -> k -> [v]
cdbGetAll cdb key = map (unpack . readData cdb) (cdbFind cdb (pack key))

-- |Returns True if the CDB has a value associated with the given key.
cdbHasKey :: (Packable k) => CDB -> k -> Bool
cdbHasKey cdb key = case cdbFind cdb (pack key) of
  [] -> False
  _  -> True

-- |Returns the number of values a CDB has for a given key.
cdbCount :: (Packable k) => CDB -> k -> Int
cdbCount cdb key = length $ cdbFind cdb (pack key)

cdbMake :: FilePath -> CDBMake -> IO ()
cdbMake fileName f = do
  let tmp = fileName <.> "tmp"
  h <- openBinaryFile tmp WriteMode
  hSeek h AbsoluteSeek (256*8)
  initState <- initialMakeState h
  cdb <- execStateT f initState
  tablesArrays <- unsafeFreeze (cdbstTables cdb)
  let tables = elems (tablesArrays :: Array Word8 [CDBSlot])
  writeHashTables h tables
  hClose h
  renameFile tmp fileName

-- |Adds a given key-value pair to a CDB
cdbAdd :: (Packable k, Packable v) => k -> v -> CDBMake
cdbAdd k v = do
  let (pk, pv) = (pack k, pack v)
  cdbAddSlot pk pv
  cdbWriteRecord pk pv

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
  pointer <- liftIO $ fromIntegral <$> hTell (cdbstHandle cdb)
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
  ibuf <- unsafeFreeze buf :: IO (Array Word32 Word32)
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

findEmptySlot :: Array Word32 Word32 -> Word32 -> Int -> Word32 -> Word32
findEmptySlot buf bufOffset tl hash =
  let tl' = fromIntegral tl
      searchStart = bufOffset + (hash `div` 256 `mod` tl') * 2
      indices = [searchStart, searchStart + 2..(tl'-1)*2] ++
                [0, 2..searchStart - 2]
      maybeSlot = find (isEmptySlot buf) indices
  in
  fromMaybe 
    (error "fatal internal error: could not find empty slot in table")
    maybeSlot

isEmptySlot :: Array Word32 Word32 -> Word32 -> Bool
isEmptySlot buf i = (buf ! (i+1)) == 0
   
writePointers :: Handle -> [(Word32, Word32)] -> IO ()
writePointers h pointers = do
  hSeek h AbsoluteSeek 0
  mapM_ (\(pointer, tableLength) -> do ByteString.hPut h (pack pointer)
                                       ByteString.hPut h (pack tableLength))
        pointers

-----------------
-- read implementation
-----------------

substr :: ByteString -> Int -> Int -> ByteString
substr bs i n = ByteString.take n (snd $ ByteString.splitAt i bs)

cdbRead32 :: CDB -> Word32 -> Word32
cdbRead32 cdb i =
  bytesToWord $ ByteString.unpack $ substr (cdbMem cdb) (fromIntegral i) 4

bytesToWord :: [Word8] -> Word32
bytesToWord = foldr (\x y -> (y `shiftL` 8) .|. fromIntegral x) 0

tableLength :: CDB -> Word8 -> Word32
tableLength cdb n = cdb `cdbRead32` ((fromIntegral n * 8) + 4)

tableOffset :: CDB -> Word8 -> Word32
tableOffset cdb n = cdb `cdbRead32` (fromIntegral n * 8)

-- gets the hash value for a key
cdbHash :: ByteString -> Word32
cdbHash =
  foldl' (\h c -> ((h `shiftL` 5) + h) `xor` fromIntegral c) 5381 .
  ByteString.unpack


-- finds the indices of hash table entries for a given key
cdbFind :: CDB -> ByteString -> [Word32]
cdbFind cdb key =
  let hash     = cdbHash key
      tableNum = fromIntegral $ hash `mod` 256
      tl       = tableLength cdb tableNum 
      in
      if tl == 0
        then []
        else
          let slotNum = hash `div` 256 `mod` tl
              linearSearch slotNum = case probe cdb tableNum slotNum of
                Just (recordOffset, hash') ->
                  let nextSlot = (slotNum + 1) `mod` tl in
                  if hash == hash' && key == readKey cdb recordOffset
                    then recordOffset : linearSearch nextSlot
                    else linearSearch nextSlot
                Nothing -> []
          in
          linearSearch slotNum

-- returns a tuple (offset, hash) if the slot contains anything
probe :: CDB -> Word8 -> Word32 -> Maybe (Word32, Word32)
probe cdb tableNum slotNum =
  let offset       = tableOffset cdb tableNum + (slotNum * 8)
      recordOffset = cdb `cdbRead32` (offset + 4)
  in
  if recordOffset == 0 then Nothing
                       else return (recordOffset, cdb `cdbRead32` offset)

readKey :: CDB -> Word32 -> ByteString
readKey cdb offset =
  let len = cdb `cdbRead32` offset in
  substr (cdbMem cdb) (fromIntegral $ offset + 8) (fromIntegral len)

readData :: CDB -> Word32 -> ByteString
readData cdb offset = 
  let keyLen = cdb `cdbRead32` offset
      dataLen = cdb `cdbRead32` (offset + 4)
  in
  substr (cdbMem cdb) (fromIntegral $ offset + 8 + keyLen)
    (fromIntegral dataLen)
