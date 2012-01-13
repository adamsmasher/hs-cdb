-- |
-- Module      : Database.CDB
-- Copyright   : (c) Adam Smith 2012
--
-- License     : BSD-style
--
-- A library for reading CDB (Constant Database) files.
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

{-# LANGUAGE FlexibleInstances #-}

module Database.CDB (
  -- * The @CDB@ type
  CDB(),
  -- * Classes
  Packable,
  Unpackable,
  -- * Interface
  cdbInit,
  cdbGet,
  cdbGetAll,
  cdbHasKey,
  cdbCount) where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as ByteString.Char8
import Data.ByteString (ByteString)
import Data.Char
import Data.List
import Data.Word
import System.IO.Posix.MMap

-- $usage
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

-----------------
-- implementation
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
  (foldl' (\h c -> ((h `shiftL` 5) + h) `xor` fromIntegral c) 5381) .
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
                  if hash == hash' && key == readKey cdb recordOffset
                    then recordOffset : (linearSearch $ slotNum + 1)
                    else linearSearch (slotNum + 1)
                Nothing -> []
          in
          linearSearch slotNum

-- returns a tuple (offset, hash) if the slot contains anything
probe :: CDB -> Word8 -> Word32 -> Maybe (Word32, Word32)
probe cdb tableNum slotNum =
  let offset       = (tableOffset cdb tableNum) + (slotNum * 8)
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
