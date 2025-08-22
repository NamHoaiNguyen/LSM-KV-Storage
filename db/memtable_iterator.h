#ifndef DB_MEMTABLE_ITERATOR_H
#define DB_MEMTABLE_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

#include <memory>
#include <string>
#include <string_view>

namespace kvs {

class SkipListNode;
class BaseMemTable;

class MemTableIterator : public BaseIterator {
public:
  MemTableIterator(const BaseMemTable *memtable);

  ~MemTableIterator() override;

  std::string_view GetKey() override;

  std::string_view GetValue() override;

  TxnId GetTransactionId() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  void Seek(std::string_view) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  const BaseMemTable *memtable_;
};

} // namespace kvs

#endif // DB_MEMTABLE_ITERATOR_H