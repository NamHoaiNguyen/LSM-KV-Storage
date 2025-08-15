#ifndef DB_MEMTABLE_ITERATOR_H
#define DB_MEMTABLE_ITERATOR_H

#include "common/base_iterator.h"

#include <memory>
#include <string>
#include <string_view>

namespace kvs {

class SkipListNode;
class MemTable;

class MemTableIterator : public BaseIterator {
public:
  MemTableIterator(MemTable* memtable);

  ~MemTableIterator() override;

  std::string GetKey() override;

  std::string GetValue() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  void Seek(std::string_view) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  const MemTable* memtable_;
};

} // namespace kvs

#endif // DB_MEMTABLE_ITERATOR_H