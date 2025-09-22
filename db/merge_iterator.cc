#include "db/merge_iterator.h"

#include "sstable/table_reader_iterator.h"

namespace kvs {

namespace db {

MergeIterator::MergeIterator(
    std::vector<std::unique_ptr<sstable::TableReaderIterator>>
        table_reader_iterators)
    : table_reader_iterators_(std::move(table_reader_iterators)),
      num_iterators_(table_reader_iterators_.size()) {}

std::string_view MergeIterator::GetKey() {}

std::string_view MergeIterator::GetValue() {}

db::ValueType MergeIterator::GetType() {}

TxnId MergeIterator::GetTransactionId() {}

bool MergeIterator::IsValid() {}

void MergeIterator::Next() {}

void MergeIterator::Prev() {}

// Jump to and load first block in table
void MergeIterator::Seek(std::string_view key) {}

void MergeIterator::SeekToFirst() {
  for (const auto iterator& : table_reader_iterators_) {
    iterator->SeekToFirst();
  }
}

void MergeIterator::SeekToLast() {
  for (const auto iterator& : table_reader_iterators_) {
    iterator->SeekToLast();
  }
}

} // namespace db

} // namespace kvs