{-# LANGUAGE FlexibleInstances, TypeSynonymInstances #-}

module Database.CDB.Packable (
  Packable,
  Unpackable,
  pack,
  unpack
) where

import Data.Array
import Data.Bits
import qualified Data.ByteString as ByteString
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as ByteString.Char8
import Data.Word

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

