#include "db/memtable.h"

#include "db/memtable_iterator.h"
#include "db/skiplist.h"

namespace kvs {

namespace db {

MemTable::MemTable() : table_(std::make_unique<SkipList>()) {}

MemTable::~MemTable() = default;

void MemTable::BatchDelete(std::span<std::string_view> keys, TxnId txn_id) {
  std::vector<std::pair<std::string, bool>> result;

  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->BatchDelete(keys, txn_id);
}

void MemTable::Delete(std::string_view key, TxnId txn_id) {
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->Delete(key, txn_id);
}

std::vector<std::pair<std::string, GetStatus>>
MemTable::BatchGet(std::span<std::string_view> keys, TxnId txn_id) {
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
  std::optional<std::string> value;

  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->Get(key, txn_id);
}

void MemTable::BatchPut(
    std::span<std::pair<std::string_view, std::string_view>> keys,
    TxnId txn_id) {
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->BatchPut(keys, txn_id);
}

void MemTable::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  table_->Put(key, value, txn_id);
}

size_t MemTable::GetMemTableSize() {
  if (!table_) {
    std::exit(EXIT_FAILURE);
  }

  return table_->GetCurrentSize();
}

const SkipList *MemTable::GetMemTable() const {
  if (!table_) {
    return nullptr;
  }
  return table_.get();
}

} // namespace db

} // namespace kvs