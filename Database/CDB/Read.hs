module Database.CDB.Read (
  CDB(),
  cdbInit,
  cdbGet,
  cdbGetAll,
  cdbHasKey,
  cdbCount
) where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as ByteString
import Data.ByteString (ByteString)
import Data.Word
import Database.CDB.Packable
import Database.CDB.Util
import System.IO.MMap

-- |Internal representation of a CDB file on disk.
data CDB = CDB { cdbMem :: ByteString }

-- |Loads a CDB from a file.
cdbInit :: FilePath -> IO CDB
cdbInit f = liftM CDB $ mmapFileByteString f Nothing

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
