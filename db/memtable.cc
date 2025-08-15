#include "db/memtable.h"

#include "db/skiplist.h"
#include "db/memtable_iterator.h"

namespace kvs {

MemTable::MemTable(size_t memtable_size)
    : memtable_size_(memtable_size), table_(std::make_unique<SkipList>()) {}

MemTable::~MemTable() = default;

MemTable::MemTable(MemTable &&other) {
  std::scoped_lock rwlock_memtable(table_mutex_, other.table_mutex_);
  std::scoped_lock rwlock_immutable_memtable(immutable_tables_mutex_,
                                             other.immutable_tables_mutex_);

  memtable_size_ = other.memtable_size_;
  table_ = std::move(other.table_);
  immutable_tables_ = std::move(other.immutable_tables_);
}

MemTable &MemTable::operator=(MemTable &&other) {
  if (this == &other) {
    return *this;
  }

  std::scoped_lock rwlock_memtable(table_mutex_, other.table_mutex_);
  std::scoped_lock rwlock_immutable_memtable(immutable_tables_mutex_,
                                             other.immutable_tables_mutex_);

  memtable_size_ = other.memtable_size_;
  table_ = std::move(other.table_);
  immutable_tables_ = std::move(other.immutable_tables_);

  return *this;
}

// std::unique_ptr<BaseIterator> MemTable::CreateNewIterator() {
//   return std::make_unique<MemTableIterator>(this);
// }

std::vector<std::pair<std::string, bool>>
MemTable::BatchDelete(std::span<std::string_view> keys, TxnId txn_id) {
  std::vector<std::pair<std::string, bool>> result;

  {
    std::scoped_lock rwlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    return table_->BatchDelete(keys, txn_id);
  }
}

bool MemTable::Delete(std::string_view key, TxnId txn_id) {
  std::scoped_lock rwlock(table_mutex_);
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->Delete(key, txn_id);
}

std::vector<std::pair<std::string, std::optional<std::string>>>
MemTable::BatchGet(std::span<std::string_view> keys, TxnId txn_id) {
  std::vector<std::pair<std::string, std::optional<std::string>>> result;
  // List of {key, index in result} keys that not be found in writable memtable
  std::vector<std::pair<std::string_view, uint32_t>> keys_not_found;
  std::optional<std::string> value;

  {
    std::shared_lock<std::shared_mutex> rlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    // First, get value from writable table
    // TODO(namnh) : should use batchGet skiplist API?
    for (std::string_view key : keys) {
      value = table_->Get(key, txn_id);
      result.push_back({std::string(key), value});
      if (!value.has_value()) {
        keys_not_found.push_back({key, result.size() - 1});
      }
    }
  }

  {
    // If key not found in writable table, continue looking up from immutable
    // tables
    std::shared_lock<std::shared_mutex> rlock(immutable_tables_mutex_);
    if (!keys_not_found.empty()) {
      return result;
    }

    for (auto [key, index] : keys_not_found) {
      for (const auto &immutable_table : immutable_tables_) {
        value = immutable_table->Get(key, txn_id);
        if (value.has_value()) {
          // If found in immutable memtables, reupdate value of key
          result[index] = std::make_pair(key, value);
        }
      }
    }
  }

  return result;
}

std::optional<std::string> MemTable::Get(std::string_view key, TxnId txn_id) {
  std::optional<std::string> value;

  {
    std::shared_lock<std::shared_mutex> rlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    // First, get value from writable table
    value = table_->Get(key, txn_id);
    if (value.has_value()) {
      return value;
    }
  }

  {
    // If key not found in writable table, continue looking up from immutable
    // tables
    std::shared_lock<std::shared_mutex> rlock(immutable_tables_mutex_);
    for (const auto &immutable_table : immutable_tables_) {
      value = immutable_table->Get(key, txn_id);
      if (value.has_value()) {
        return value;
      }
    }
  }

  return std::nullopt;
}

void MemTable::BatchPut(
    std::span<std::pair<std::string_view, std::string_view>> keys,
    TxnId txn_id) {
  {
    std::scoped_lock rwlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    table_->BatchPut(keys, txn_id);
  }
  {
    std::scoped_lock rwlock(immutable_tables_mutex_);
    if (table_->GetCurrentSize() >= memtable_size_) {
      // Convert table_ to immutable memtable and add to immutable list.
      CreateNewMemtable();
    }
  }
}

void MemTable::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  {
    std::scoped_lock rwlock(table_mutex_);
    if (!table_) {
      std::exit(EXIT_FAILURE);
    }

    table_->Put(key, value, txn_id);
  }
  {
    std::scoped_lock rwlock(immutable_tables_mutex_);
    if (table_->GetCurrentSize() >= memtable_size_) {
      // Convert table_ to immutable memtable and add to immutable list.
      CreateNewMemtable();
    }
  }
}

// NOT THREAD-SAFE. Lock must be acquired before this method is called.
void MemTable::CreateNewMemtable() {
  immutable_tables_.push_back(std::move(table_));
  table_ = std::make_unique<SkipList>();
}

} // namespace kvs