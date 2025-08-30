#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "common/macros.h"
#include "db/base_memtable.h"
#include "db/status.h"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string_view>

namespace kvs {

class BaseIterator;
class SkipList;

class MemTable : public BaseMemTable {
public:
  MemTable();

  ~MemTable() override;

  // Copy constructor/assignment
  MemTable(const MemTable &) = delete;
  MemTable &operator=(MemTable &) = delete;

  // Move constructor/assignment
  MemTable(MemTable &&);
  MemTable &operator=(MemTable &&);

  std::unique_ptr<BaseIterator> CreateNewIterator();

  std::vector<std::pair<std::string, bool>>
  BatchDelete(std::span<std::string_view> keys, TxnId txn_id) override;

  bool Delete(std::string_view key, TxnId txn_id) override;

  virtual std::vector<std::pair<std::string, GetStatus>>
  BatchGet(std::span<std::string_view> keys, TxnId txn_id) override;

  GetStatus Get(std::string_view key, TxnId txn_id) override;

  void BatchPut(std::span<std::pair<std::string_view, std::string_view>> keys,
                TxnId txn_id) override;

  void Put(std::string_view key, std::string_view value, TxnId txn_id) override;

  bool IsImmutable() override;

  void SetImmutable() override;

  size_t GetMemTableSize() override;

  const SkipList *GetMemTable() const override;

private:
  bool is_immutable_;

  std::unique_ptr<SkipList> table_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H