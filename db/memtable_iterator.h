#ifndef DB_MEMTABLE_ITERATOR_H
#define DB_MEMTABLE_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace kvs {

namespace db {

class SkipListIterator;
class BaseMemTable;

class MemTableIterator : public kvs::BaseIterator {
public:
  explicit MemTableIterator(const BaseMemTable *const memtable);

  ~MemTableIterator() override;

  std::string_view GetKey() override;

  std::optional<std::string_view> GetValue() override;

  ValueType GetType() override;

  TxnId GetTransactionId() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  std::unique_ptr<SkipListIterator> iterator_;
};

} // namespace db

} // namespace kvs

#endif // DB_MEMTABLE_ITERATOR_H