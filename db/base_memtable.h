#ifndef DB_BASE_MEMTABLE_H
#define DB_BASE_MEMTABLE_H

#include "common/macros.h"
#include "db/status.h"

#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace kvs {

namespace db {

class SkipList;

class BaseMemTable {
public:
  virtual ~BaseMemTable() = default;

  virtual void BatchDelete(std::span<std::string_view>, TxnId txn_id) = 0;

  virtual void Delete(std::string_view key, TxnId txn_id) = 0;

  virtual std::vector<std::pair<std::string, GetStatus>>
  BatchGet(std::span<std::string_view>, TxnId txn_id) = 0;

  virtual GetStatus Get(std::string_view key, TxnId txn_id) = 0;

  virtual void
  BatchPut(std::span<std::pair<std::string_view, std::string_view>> keys,
           TxnId txn_id) = 0;

  virtual void Put(std::string_view key, std::string_view value,
                   TxnId txn_id) = 0;

  virtual size_t GetMemTableSize() const = 0;

  virtual const SkipList *GetMemTable() const = 0;
};

} // namespace db

} // namespace kvs

#endif // DB_BASE_MEMTABLE_H