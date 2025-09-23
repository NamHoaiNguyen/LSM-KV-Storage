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
    HeapItem new_heap_item(heap_item.iterator->GetKey(),
                           heap_item.iterator->GetTransactionId(),
                           heap_item.iterator);
    min_heap_.push(new_heap_item);
  }
}

void MergeIterator::Prev() {
  HeapItem heap_item = max_heap_.top();
  max_heap_.pop();

  heap_item.iterator->Prev();
  if (heap_item.iterator->IsValid()) {
    // re-push into heap if table iterator is still valid
    HeapItem new_heap_item(heap_item.iterator->GetKey(),
                           heap_item.iterator->GetTransactionId(),
                           heap_item.iterator);
    max_heap_.push(new_heap_item);
  }
}

// Jump to and load first block in table
void MergeIterator::Seek(std::string_view key) {
  // Clear data of min_heap_ each "Seek"
  std::priority_queue<HeapItem, std::vector<HeapItem>, LessCompare> pq;
  min_heap_.swap(pq);

  for (const auto &iterator : table_reader_iterators_) {
    iterator->Seek(key);

    // build HeapItem for each table iterator
    HeapItem heap_item(iterator->GetKey(), iterator->GetTransactionId(),
                       iterator.get());
    // Add item into min heap
    min_heap_.push(heap_item);
  }
}

void MergeIterator::SeekToFirst() {
  // Clear data of min_heap_ each "Seek"
  std::priority_queue<HeapItem, std::vector<HeapItem>, LessCompare> pq;
  min_heap_.swap(pq);

  for (auto &iterator : table_reader_iterators_) {
    iterator->SeekToFirst();
    // build HeapItem for each table iterator
    HeapItem heap_item(iterator->GetKey(), iterator->GetTransactionId(),
                       iterator.get());
    // Add item into min heap
    min_heap_.push(heap_item);
  }
}

void MergeIterator::SeekToLast() {
  // Clear data of min_heap_ each "Seek"
  // Clear data of min_heap_ each "Seek"
  std::priority_queue<HeapItem, std::vector<HeapItem>, GreaterCompare> pq;
  max_heap_.swap(pq);

  for (const auto &iterator : table_reader_iterators_) {
    iterator->SeekToLast();
    // build HeapItem for each table iterator
    HeapItem heap_item(iterator->GetKey(), iterator->GetTransactionId(),
                       iterator.get());
    // Add item into min heap
    max_heap_.push(heap_item);
  }
}

} // namespace db

} // namespace kvs