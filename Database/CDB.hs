module Database.CDB (cdbInit, cdbGet, cdbGetAll) where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as ByteString
import Data.ByteString (ByteString)
import Data.Char
import Data.List
import Data.Word
import System.IO.Posix.MMap

data CDB = CDB { cdbMem :: ByteString }

substr :: ByteString -> Int -> Int -> ByteString
substr bs i n = ByteString.take n (snd $ ByteString.splitAt i bs)

cdbRead32 :: CDB -> Word32 -> Word32
cdbRead32 cdb i =
  bytesToInt $ ByteString.unpack $ substr (cdbMem cdb) (fromIntegral i) 4

--bytesToInt :: [Word8] -> Word32
bytesToInt = foldr (\x y -> (y `shiftL` 8) .|. fromIntegral x) 0

tableLength :: CDB -> Word8 -> Word32
tableLength cdb n = cdb `cdbRead32` ((fromIntegral n * 8) + 4)

tableOffset :: CDB -> Word8 -> Word32
tableOffset cdb n = cdb `cdbRead32` (fromIntegral n * 8)

-- |Loads a CDB from a file
cdbInit :: FilePath -> IO CDB
cdbInit f = liftM CDB $ unsafeMMapFile f 

-- gets the hash value for a key
cdbHash :: ByteString -> Word32
cdbHash =
  (foldl' (\h c -> ((h `shiftL` 5) + h) `xor` fromIntegral c) 5381) .
  ByteString.unpack

-- |Finds the first entry associated with a key in a CDB
cdbGet    :: CDB -> ByteString -> Maybe ByteString
cdbGet cdb key = case cdbFind cdb key of
  []    -> Nothing
  (x:_) -> return $ readData cdb x 

-- |Finds all entries associated with a key in a CDB
cdbGetAll :: CDB -> ByteString -> [ByteString]
cdbGetAll cdb key = map (readData cdb) (cdbFind cdb key)
 
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
