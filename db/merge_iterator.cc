#include "db/merge_iterator.h"

#include "sstable/block_reader_iterator.h"
#include "sstable/table_reader_iterator.h"

namespace kvs {

namespace db {

MergeIterator::MergeIterator(
    std::vector<std::unique_ptr<sstable::TableReaderIterator>>
        table_reader_iterators)
    : table_reader_iterators_(std::move(table_reader_iterators)),
      num_iterators_(table_reader_iterators_.size()) {}

std::string_view MergeIterator::GetKey() {
  return min_heap_.top().iterator->GetKey();
}

std::string_view MergeIterator::GetValue() {
  return min_heap_.top().iterator->GetValue();
}

db::ValueType MergeIterator::GetType() {
  return min_heap_.top().iterator->GetType();
}

TxnId MergeIterator::GetTransactionId() {
  return min_heap_.top().iterator->GetTransactionId();
}

bool MergeIterator::IsValid() { return !min_heap_.empty(); }

void MergeIterator::Next() {
  HeapItem heap_item = min_heap_.top();
  min_heap_.pop();

  heap_item.iterator->Next();
  if (heap_item.iterator->IsValid()) {
    // re-push into heap if table iterator is still valid
    min_heap_.push(heap_item);
  }
}

void MergeIterator::Prev() {
  HeapItem heap_item = min_heap_.top();
  min_heap_.pop();

  heap_item.iterator->Prev();
  if (heap_item.iterator->IsValid()) {
    // re-push into heap if table iterator is still valid
    min_heap_.push(heap_item);
  }
}

// Jump to and load first block in table
void MergeIterator::Seek(std::string_view key) {}

void MergeIterator::SeekToFirst() {
  for (const auto &iterator : table_reader_iterators_) {
    // build HeapItem for each table iterator
    HeapItem heap_item(iterator->GetKey(), iterator->GetTransactionId(),
                       iterator.get());
    // Add item into min heap
    min_heap_.push(heap_item);

    iterator->SeekToFirst();
  }
}

void MergeIterator::SeekToLast() {
  for (const auto &iterator : table_reader_iterators_) {
    // build HeapItem for each table iterator
    HeapItem heap_item(iterator->GetKey(), iterator->GetTransactionId(),
                       iterator.get());
    // Add item into min heap
    min_heap_.push(heap_item);

    iterator->SeekToLast();
  }
}

} // namespace db

} // namespace kvs