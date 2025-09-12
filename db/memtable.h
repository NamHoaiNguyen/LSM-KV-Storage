#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "common/macros.h"
#include "db/base_memtable.h"
#include "db/status.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string_view>

namespace kvs {

namespace db {

class BaseIterator;
class SkipList;

class MemTable : public BaseMemTable {
public:
  MemTable();

  ~MemTable() override = default;

  // Copy constructor/assignment
  MemTable(const MemTable &) = delete;
  MemTable &operator=(MemTable &) = delete;

  // Move constructor/assignment
  MemTable(MemTable &&) = default;
  MemTable &operator=(MemTable &&) = default;

  std::unique_ptr<BaseIterator> CreateNewIterator();

  void BatchDelete(std::span<std::string_view> keys, TxnId txn_id) override;

  void Delete(std::string_view key, TxnId txn_id) override;

  virtual std::vector<std::pair<std::string, GetStatus>>
  BatchGet(std::span<std::string_view> keys, TxnId txn_id) override;

  GetStatus Get(std::string_view key, TxnId txn_id) override;

  void BatchPut(std::span<std::pair<std::string_view, std::string_view>> keys,
                TxnId txn_id) override;

  void Put(std::string_view key, std::string_view value, TxnId txn_id) override;

  size_t GetMemTableSize() override;

  const SkipList *GetMemTable() const override;

private:
  std::atomic<bool> is_flushing_;

  std::atomic<uint64_t> sequence_number_;

  std::unique_ptr<SkipList> table_;
};

} // namespace db

} // namespace kvs

#endif // DB_MEMTABLE_H