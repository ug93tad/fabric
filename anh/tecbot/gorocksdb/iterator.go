package gorocksdb 

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"bytes"
	"errors"
	"unsafe"
  "github.com/hyperledger/fabric/ustore"
)

// Iterator provides a way to seek to specific keys and iterate through
// the keyspace from that point, as well as access the values of those keys.
//
// For example:
//
//      it := db.NewIterator(readOpts)
//      defer it.Close()
//
//      it.Seek([]byte("foo"))
//		for ; it.Valid(); it.Next() {
//          fmt.Printf("Key: %v Value: %v\n", it.Key().Data(), it.Value().Data())
// 		}
//
//      if err := it.Err(); err != nil {
//          return err
//      }
//
type Iterator struct {
	c *C.rocksdb_iterator_t
  ui  ustore.Iterator
}

// NewNativeIterator creates a Iterator object.
func NewNativeIterator(c unsafe.Pointer) *Iterator {
	return &Iterator{(*C.rocksdb_iterator_t)(c), nil}
}

func NewUStoreIterator(ut ustore.Iterator) *Iterator {
  return &Iterator{nil, ut}
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *Iterator) Valid() bool {
  if iter.c != nil {
	  return C.rocksdb_iter_valid(iter.c) != 0
  } else {
    return iter.ui.Valid()
  }
}

// ValidForPrefix returns false only when an Iterator has iterated past the
// first or the last key in the database or the specified prefix.
func (iter *Iterator) ValidForPrefix(prefix []byte) bool {
  if iter.ui != nil {
    panic("Not implemented in UStore")
  }
	return C.rocksdb_iter_valid(iter.c) != 0 && bytes.HasPrefix(iter.Key().Data(), prefix)
}

// Key returns the key the iterator currently holds.
func (iter *Iterator) Key() *Slice {
  if iter.ui != nil {
    return NewUStoreSlice(iter.ui.Key())
  }
	var cLen C.size_t
	cKey := C.rocksdb_iter_key(iter.c, &cLen)
	if cKey == nil {
		return nil
	}
	return &Slice{cKey, cLen, true, nil}
}

// Value returns the value in the database the iterator currently holds.
func (iter *Iterator) Value() *Slice {
  if iter.ui != nil {
    return NewUStoreSlice(iter.ui.Value())
  }
	var cLen C.size_t
	cVal := C.rocksdb_iter_value(iter.c, &cLen)
	if cVal == nil {
		return nil
	}
	return &Slice{cVal, cLen, true, nil}
}

// Next moves the iterator to the next sequential key in the database.
func (iter *Iterator) Next() {
  if iter.ui != nil {
    iter.ui.Next()
  } else {
	  C.rocksdb_iter_next(iter.c)
  }
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *Iterator) Prev() {
  if iter.ui != nil {
    iter.ui.Prev()
  } else {
	  C.rocksdb_iter_prev(iter.c)
  }
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *Iterator) SeekToFirst() {
  if iter.ui != nil {
    iter.ui.SeekToFirst()
  } else {
	  C.rocksdb_iter_seek_to_first(iter.c)
  }
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *Iterator) SeekToLast() {
  if iter.ui != nil {
    iter.ui.SeekToLast()
  } else {
	  C.rocksdb_iter_seek_to_last(iter.c)
  }
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *Iterator) Seek(key []byte) {
  if iter.ui != nil {
    iter.ui.Seek(string(key[:]))
  } else {
	  cKey := byteToChar(key)
	  C.rocksdb_iter_seek(iter.c, cKey, C.size_t(len(key)))
  }
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (iter *Iterator) Err() error {
  if iter.ui != nil {
    panic("Not implemented in UStore")
  }
	var cErr *C.char
	C.rocksdb_iter_get_error(iter.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Close closes the iterator.
func (iter *Iterator) Close() {
  if iter.ui != nil {
    iter.ui.Release()
  } else {
	  C.rocksdb_iter_destroy(iter.c)
	  iter.c = nil
  }
}
