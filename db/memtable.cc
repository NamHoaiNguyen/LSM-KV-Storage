#include "db/memtable.h"

#include "db/memtable_iterator.h"
#include "db/skiplist.h"

namespace kvs {

MemTable::MemTable()
    : is_immutable_(false), table_(std::make_unique<SkipList>()) {}

MemTable::~MemTable() = default;

MemTable::MemTable(MemTable &&other) {
  std::scoped_lock rwlock_memtable(mutex_, other.mutex_);
  is_immutable_ = other.is_immutable_;
  table_ = std::move(other.table_);
}

MemTable &MemTable::operator=(MemTable &&other) {
  if (this == &other) {
    return *this;
  }

  std::scoped_lock rwlock_memtable(mutex_, other.mutex_);
  is_immutable_ = other.is_immutable_;
  table_ = std::move(other.table_);

  return *this;
}

// std::unique_ptr<BaseIterator> MemTable::CreateNewIterator() {
//   return std::make_unique<MemTableIterator>(this);
// }

std::vector<std::pair<std::string, bool>>
MemTable::BatchDelete(std::span<std::string_view> keys, TxnId txn_id) {
  std::scoped_lock rwlock(mutex_);
  if (is_immutable_) {
    throw std::runtime_error("Can't delete in immutable memtable!!!");
  }

  std::vector<std::pair<std::string, bool>> result;

  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->BatchDelete(keys, txn_id);
}

bool MemTable::Delete(std::string_view key, TxnId txn_id) {
  std::scoped_lock rwlock(mutex_);
  if (is_immutable_) {
    throw std::runtime_error("Can't delete in immutable memtable!!!");
  }
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->Delete(key, txn_id);
}

std::vector<std::pair<std::string, GetStatus>>
MemTable::BatchGet(std::span<std::string_view> keys, TxnId txn_id) {
  std::shared_lock<std::shared_mutex> rlock(mutex_);
  std::vector<std::pair<std::string, GetStatus>> result;

  // List of {key, index in result} keys that not be found in writable memtable
  std::vector<std::pair<std::string_view, uint32_t>> keys_not_found;
  GetStatus status;

  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  // First, get value from writable table
  // TODO(namnh) : should use batchGet skiplist API?
  for (std::string_view key : keys) {
    status = table_->Get(key, txn_id);
    result.push_back({std::string(key), std::move(status)});
    // if (!value.has_value()) {
    //   keys_not_found.push_back({key, result.size() - 1});
    // }
  }

  return result;
}

GetStatus MemTable::Get(std::string_view key, TxnId txn_id) {
  std::shared_lock<std::shared_mutex> rlock(mutex_);
  std::optional<std::string> value;

  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->Get(key, txn_id);
}

void MemTable::BatchPut(
    std::span<std::pair<std::string_view, std::string_view>> keys,
    TxnId txn_id) {
  std::scoped_lock rwlock(mutex_);
  if (is_immutable_) {
    throw std::runtime_error("Can't BATCH PUT in immutable memtable!!!");
  }
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->BatchPut(keys, txn_id);
}

void MemTable::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  std::scoped_lock rwlock(mutex_);
  if (is_immutable_) {
    throw std::runtime_error("Can't PUT in immutable memtable!!!");
  }
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->Put(key, value, txn_id);
}

size_t MemTable::GetMemTableSize() {
  std::shared_lock<std::shared_mutex> rlock(mutex_);
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->GetCurrentSize();
}

const SkipList *MemTable::GetMemTable() const {
  std::shared_lock<std::shared_mutex> rlock(mutex_);
  return table_.get();
}

bool MemTable::IsImmutable() {
  std::shared_lock<std::shared_mutex> rlock(mutex_);
  return is_immutable_;
}

void MemTable::SetImmutable() {
  std::scoped_lock rwlock(mutex_);
  is_immutable_ = true;
}

} // namespace kvs