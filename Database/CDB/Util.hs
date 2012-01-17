module Database.CDB.Util (cdbHash) where

import qualified Data.ByteString as ByteString
import Data.ByteString (ByteString)
import Data.Bits
import Data.List
import Data.Word

-- gets the hash value for a key
cdbHash :: ByteString -> Word32
cdbHash =
  foldl' (\h c -> ((h `shiftL` 5) + h) `xor` fromIntegral c) 5381 .
  ByteString.unpack
