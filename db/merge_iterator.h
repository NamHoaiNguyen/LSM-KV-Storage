#ifndef DB_MERGE_ITERATOR_H
#define DB_MERGE_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

// libC++
#include <cassert>
#include <memory>
#include <queue>
#include <vector>

namespace kvs {

namespace sstable {
class TableReaderIterator;
}

namespace db {

class MergeIterator : public kvs::BaseIterator {
public:
  MergeIterator(std::vector<std::unique_ptr<sstable::TableReaderIterator>>);

  ~MergeIterator() = default;

  // No copy allowed
  MergeIterator(const MergeIterator &) = delete;
  MergeIterator &operator=(MergeIterator &) = delete;

  // Move constructor/assignment
  MergeIterator(MergeIterator &&) = default;
  MergeIterator &operator=(MergeIterator &&) = default;

  std::string_view GetKey() override;

  std::string_view GetValue() override;

  db::ValueType GetType() override;

  TxnId GetTransactionId() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  // Jump  to and load first block in table
  void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  const std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators_;

  const size_t num_iterators_;

  std::priority_queue<std::string, std::vector<std::string>,
                      std::greater<std::string>> min_heap_;
};

} // namespace db

} // namespace kvs

#endif // DB_MERGE_ITERATOR_H