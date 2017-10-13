%module ustore
%include <std_string.i>
%include <std_pair.i>

%{
#include "db.h"
%}

%template(PairStatusString) std::pair<ustore_kvdb::Status, std::string>;

namespace ustore_kvdb {

%nodefaultctor Iterator;
class Iterator;

class KVDB {
 public:
  explicit KVDB(unsigned int id = 42);

  std::pair<Status, std::string> Get(const std::string& key);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Write(WriteBatch* updates);
  bool Exist(const std::string& key);
  Iterator* NewIterator();
  size_t GetSize();
};

class Iterator {
 public:
  virtual ~Iterator();
  
  void Release();
  virtual void SetRange(const std::string& a, const std::string& b);
  virtual bool Valid();
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const std::string& key);
  virtual bool Next();
  virtual bool Prev();
  virtual std::string key() const;
  virtual std::string value() const;
};

extern Iterator* NewEmptyIterator();

}
