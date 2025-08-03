#include "memtable.h"

#include "skiplist.h"

namespace kvs {

MemTable::MemTable(size_t memtable_size)
    : memtable_size_(memtable_size), table_(std::make_unique<SkipList>()) {}

MemTable::~MemTable() = default;

void MemTable::Delete(std::string_view key) {}

std::optional<std::string> MemTable::Get(std::string_view key, TxnId txn_id) {
  {
    std::shared_lock<std::shared_mutex> rlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    // First, get value from writing table
    std::optional<std::string> value;
    value = table_->Get(key, txn_id);
    if (value.has_value()) {
      return value;
    }
  }

  // If key not found in writing table, continue looking up from immutable
  // tables
  {
    std::shared_lock<std::shared_mutex> rlock(immutable_tables_mutex_);
    std::optional<std::string> value;
    for (const auto &frozen_table : immutable_tables_) {
      value = table_->Get(key, txn_id);
      if (value.has_value()) {
        return value;
      }
    }
  }

  // If key still can't be found from immutable tables, load from SSTs
  // TODO(namnh2) : Implement SST.
  return std::nullopt;
}

void MemTable::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  {
    std::scoped_lock<std::shared_mutex> rwlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    table_->Put(key, value, txn_id);
    {
      std::scoped_lock<std::shared_mutex> rwlock(immutable_tables_mutex_);
      if (table_->GetCurrentSize() >= memtable_size_) {
        // Convert table_ to immutable memtable and add to immutable list.
        CreateNewMemtable();
      }
    }
  }
}

// NOT THREAD-SAFE. Lock must be acquired before this method is called.
void MemTable::CreateNewMemtable() {
  immutable_tables_.push_back(std::move(table_));
  table_ = std::make_unique<SkipList>();
}

} // namespace kvs