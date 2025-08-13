#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "common/macros.h"
#include "db/base_memtable.h"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string_view>

namespace kvs {

class SkipList;

class MemTable : public BaseMemtable {
public:
  MemTable(size_t memtable_size = 8 * 1024 * 1024 /*MB*/);

  ~MemTable() override;

  // Copy constructor/assignment
  MemTable(const MemTable &) = delete;
  MemTable &operator=(MemTable &) = delete;

  // Move constructor/assignment
  MemTable(MemTable &&);
  MemTable &operator=(MemTable &&);

  std::vector<std::pair<std::string, std::optional<std::string>>>
  BatchGet(std::span<std::string_view> keys, TxnId txn_id) override;

  std::optional<std::string> Get(std::string_view key, TxnId txn_id) override;

  void BatchPut(std::span<std::pair<std::string_view, std::string_view>> keys,
                TxnId txn_id) override;

  void Put(std::string_view key, std::string_view value, TxnId txn_id) override;

  std::vector<std::pair<std::string, bool>>
  BatchDelete(std::span<std::string_view> keys, TxnId txn_id) override;

  bool Delete(std::string_view key, TxnId txn_id) override;

private:
  void CreateNewMemtable();

  size_t memtable_size_;

  std::unique_ptr<SkipList> table_;

  std::vector<std::unique_ptr<SkipList>> immutable_tables_;

  std::shared_mutex table_mutex_;

  std::shared_mutex immutable_tables_mutex_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H