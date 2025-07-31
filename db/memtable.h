#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "common/macros.h"
#include "db/base_memtable.h"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <vector>

namespace kvs {

class SkipList;

class MemTable : public BaseMemtable {
public:
  MemTable();

  ~MemTable() override;

  void BatchGet() override;

  void Get(std::string_view key, TxnId txn_id) override;

  void BatchPut() override;

  void Put(std::string_view key, std::string_view value, TxnId txn_id) override;

  void Delete() override;

private:
  void CreateNewMemtable();

  // TODO(namnh) : unique_ptr or shared_ptr
  std::unique_ptr<SkipList> table_;

  std::vector<std::unique_ptr<SkipList>> immutable_tables_;

  std::shared_mutex table_mutex_;

  std::shared_mutex immutable_tables_mutex_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H