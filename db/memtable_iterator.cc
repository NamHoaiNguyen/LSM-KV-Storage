#include "db/memtable_iterator.h"

#include "common/base_iterator.h"
#include "db/base_memtable.h"
#include "db/skiplist_iterator.h"

namespace kvs {

namespace db {

MemTableIterator::MemTableIterator(const BaseMemTable *const memtable)
    : iterator_(std::make_unique<SkipListIterator>(memtable->GetMemTable())) {}

std::string_view MemTableIterator::GetKey() { return iterator_->GetKey(); }

std::optional<std::string_view> MemTableIterator::GetValue() {
  return iterator_->GetValue();
}

ValueType MemTableIterator::GetType() { return iterator_->GetType(); }

TxnId MemTableIterator::GetTransactionId() {
  return iterator_->GetTransactionId();
}

bool MemTableIterator::IsValid() { return iterator_->IsValid(); }

void MemTableIterator::Next() { iterator_->Next(); }

void MemTableIterator::Prev() { iterator_->Prev(); }

void MemTableIterator::Seek(std::string_view key) { iterator_->Seek(key); }

void MemTableIterator::SeekToFirst() { iterator_->SeekToFirst(); }

void MemTableIterator::SeekToLast() { iterator_->SeekToLast(); }

} // namespace db

} // namespace kvs