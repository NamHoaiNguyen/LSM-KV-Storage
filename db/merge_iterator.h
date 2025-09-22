#ifndef DB_MERGE_ITERATOR_H
#define DB_MERGE_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

// libC++
#include <cassert>
#include <memory>
#include <queue>
#include <string>
#include <vector>

namespace kvs {

namespace sstable {
class TableReaderIterator;
}

namespace db {

class MergeIterator : public kvs::BaseIterator {
public:
  MergeIterator(std::vector<std::unique_ptr<sstable::TableReaderIterator>>
                    table_reader_iterators);

  ~MergeIterator() = default;

  // No copy allowed
  MergeIterator(const MergeIterator &) = delete;
  MergeIterator &operator=(MergeIterator &) = delete;

  // Move constructor/assignment
  MergeIterator(MergeIterator &&) = default;
  MergeIterator &operator=(MergeIterator &&) = default;

  // Return the smallest key(which is the top of min heap)
  std::string_view GetKey() override;

  // Return value of smallest key
  std::string_view GetValue() override;

  // Return type of smallest key
  db::ValueType GetType() override;

  // Return transaction id of smallest key
  TxnId GetTransactionId() override;

  bool IsValid() override;

  // Get table iterator that have, currently, the smallest key then move it
  // forward
  void Next() override;

  // Get table iterator that have, currently, the smallest key then move it
  // backward
  void Prev() override;

  // Jump  to and load first block in table
  void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  struct HeapItem {
    HeapItem(std::string_view key_item, TxnId txn_id_item,
             sstable::TableReaderIterator *iterator_item)
        : key(key_item), txn_id(txn_id_item), iterator(iterator_item) {}

    // Copy constructor/assignment
    HeapItem(const HeapItem &) = default;
    HeapItem &operator=(HeapItem &) = default;

    // Move constructor/assignment
    HeapItem(HeapItem &&) = default;
    HeapItem &operator=(HeapItem &&) = default;

    std::string_view key;

    TxnId txn_id;

    sstable::TableReaderIterator *iterator;
  };

  struct Compare {
    bool operator()(const HeapItem &a, const HeapItem &b) {
      return a.key > b.key || (a.key == b.key && a.txn_id > b.txn_id);
    }
  };

  const std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators_;

  const size_t num_iterators_;

  std::priority_queue<HeapItem, std::vector<HeapItem>, Compare> min_heap_;
};

} // namespace db

} // namespace kvs

#endif // DB_MERGE_ITERATOR_H