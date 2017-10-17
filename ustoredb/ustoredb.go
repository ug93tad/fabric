// DB wrapper for USTORE
package ustoredb

import (
	"fmt"
	"github.com/hyperledger/fabric/ustore"
  "sync"
)

// column family namespace
type ColumnFamilyHandle struct {
  db ustore.KVDB // pointer to DB partition
  name  string
}

// wrap write batch, indexed by ColumnFamily name
type WriteBatch struct {
  updates map[string]ustore.WriteBatch
}

type UStoreDB struct {
  db ustore.KVDB  // default DB partition
	cFamilies map[string]*ColumnFamilyHandle
  ncfs      uint32 // number of column families
}


var once sync.Once

func OpenDB() (*UStoreDB, error) {
    db := ustore.NewKVDB(uint(0))
    return  &UStoreDB{db, make(map[string]*ColumnFamilyHandle), 1}, nil
}

func Close(db *UStoreDB) {
  ustore.DeleteKVDB(db.db)

  /*
  for cf := range db.cFamilies {
    delete(db.cFamilies, cf)
  }*/
}

func NewWriteBatch() (*WriteBatch, error) {
  return &WriteBatch{make(map[string]ustore.WriteBatch)}, nil
}

func DeleteWriteBatch(batch *WriteBatch) {
  for _, b := range(batch.updates) {
    ustore.DeleteWriteBatch(b)
  }
}

func GetIterator(cfh *ColumnFamilyHandle) (ustore.Iterator, error) {
  return cfh.db.NewIterator(), nil
}

func (cfh *ColumnFamilyHandle) GetCFName() string {
  return cfh.name
}

func DeleteIterator(it ustore.Iterator) {
  ustore.DeleteIterator(it)
}

func (writebatch *WriteBatch) DeleteCF(cfh *ColumnFamilyHandle, key string) {
  if wb, ok := writebatch.updates[cfh.name]; ok {
    wb.Delete(key)
  }
}

func (writebatch *WriteBatch) Clear() {
  for _,wb := range writebatch.updates {
    wb.Clear()
  }
}

func (writebatch *WriteBatch) PutCF(cfh *ColumnFamilyHandle, key string, value string) error {
  // gathering updates
  if wb, ok := writebatch.updates[cfh.name]; ok {
    // CF existed
    wb.Put(key, value)
  } else {
    tmp := ustore.NewWriteBatch()
    tmp.Put(key, value)
    writebatch.updates[cfh.name] =  tmp
  }
  return nil
}

func (db *UStoreDB) GetSize() uint64 {
  return uint64(db.db.GetSize())
}
func (db *UStoreDB) CreateColumnFamily(cfname string) (*ColumnFamilyHandle, error) {
  if _, ok := db.cFamilies[cfname]; ok {
    return nil, fmt.Errorf("Column family %v already existed", cfname)
  } else {
    cfh := &ColumnFamilyHandle{ustore.NewKVDB(uint(db.ncfs)), cfname}
    db.ncfs++
    db.cFamilies[cfname] = cfh
    return cfh, nil
  }
}

func (db *UStoreDB) DropColumnFamily(cfh *ColumnFamilyHandle) error {
  delete(db.cFamilies, cfh.name)
  return nil
}

func DeleteColumnFamilyHandle(cfh *ColumnFamilyHandle) {
  ustore.DeleteKVDB(cfh.db)
}

func (db *UStoreDB) PutCF(cfh *ColumnFamilyHandle, key string, value string) error {
  if err := cfh.db.Put(key, value); err.Ok() {
    return nil
  } else {
    return fmt.Errorf("Error during Put")
  }
}

func (db *UStoreDB) Write(writebatch *WriteBatch) error {
  for k, v := range(writebatch.updates) {
    db.cFamilies[k].db.Write(v)
  }
  return nil
}

func (db *UStoreDB) DeleteCF(cfh *ColumnFamilyHandle, key string) error {
  if err := cfh.db.Delete(key); err.Ok() {
    return nil
  } else {
    return fmt.Errorf("Error during Delete")
  }
}

func (db *UStoreDB) ExistCF(cfh *ColumnFamilyHandle, key string) error {
  if err := cfh.db.Exist(key); err {
    return nil
  } else {
    return fmt.Errorf("Error during Exist")
  }
}

func (db *UStoreDB) GetCF(cfh *ColumnFamilyHandle, key string) (string, error) {
  if err := cfh.db.Get(key); err.GetFirst().Ok() {
    return err.GetSecond(), nil
  } else {
    return "", fmt.Errorf("Error during Get")
  }
}
