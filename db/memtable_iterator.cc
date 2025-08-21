#include "db/memtable_iterator.h"

#include "common/base_iterator.h"
#include "db/base_memtable.h"

namespace kvs {

MemTableIterator::MemTableIterator(const BaseMemTable *memtable)
    : memtable_(memtable) {}

MemTableIterator::~MemTableIterator() = default;

std::string_view MemTableIterator::GetKey() { return ""; }

std::string_view MemTableIterator::GetValue() { return ""; }

TxnId MemTableIterator::GetTransactionId() { return 0; }

bool MemTableIterator::IsValid() { return false; }

void MemTableIterator::Next() {}

void MemTableIterator::Prev() {}

void MemTableIterator::Seek(std::string_view) {}

void MemTableIterator::SeekToFirst() {}

void MemTableIterator::SeekToLast() {}

} // namespace kvs