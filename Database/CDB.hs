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


module Database.CDB (
  -- * The @CDB@ type
  CDB(),
  -- * Classes
  Packable,
  Unpackable,
  pack,
  unpack,
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

import Database.CDB.Read
import Database.CDB.Write
import Database.CDB.Packable
