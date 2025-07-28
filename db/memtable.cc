#include "db/memtable.h"

#include "db/skiplist.h"

MemTable::Memtable() : 
  table_(std::make_unique<SkipList>()) {}

MemTable::~Memtable() = default;

void MemTable::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->Put(key, value, txn_id);
}