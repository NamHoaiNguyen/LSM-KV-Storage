#ifndef DB_BASE_MEMTABLE_H
#define DB_BASE_MEMTABLE_H

#include "common/macros.h"

#include <string_view>

namespace kvs {

class BaseMemtable {
public:
  virtual ~BaseMemtable() = default;

  virtual void BatchGet() = 0;

  virtual void Get(std::string_view key, TxnId txn_id) = 0;

  virtual void BatchPut() = 0;

  virtual void Put(std::string_view key, std::string_view value,
                   TxnId txn_id) = 0;

  virtual void Delete() = 0;

private:
};

} // namespace kvs

#endif // DB_BASE_MEMTABLE_H