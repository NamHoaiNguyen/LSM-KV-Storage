#include "memtable.h"

#include "skiplist.h"

namespace kvs {

// TODO(namnh) : config instead of hard set
constexpr int max_memtable_size = 64 * 1024 * 1024; // MB

MemTable::MemTable() : table_(std::make_unique<SkipList>()) {}

MemTable::~MemTable() = default;

void MemTable::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  {
    std::scoped_lock<std::shared_mutex> rwlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    table_->Put(key, value, txn_id);
    {
      std::scoped_lock<std::shared_mutex> rwlock(immutable_tables_mutex_);
      if (table_->GetCurrentSize() >= max_memtable_size) {
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

void MemTable::Get(std::string_view key, TxnId txn_id) {
  {
    std::shared_lock<std::shared_mutex> rlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    table_->Get(key, txn_id);
  }
}

} // namespace kvs