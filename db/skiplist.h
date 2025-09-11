#ifndef DB_SKIPLIST_H
#define DB_SKIPLIST_H

#include "common/macros.h"
#include "db/status.h"

#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>

namespace kvs {

namespace db {

class SkipListIterator;
class SkipListNode;

// ALL methods in this class ARE NOT THREAD-SAFE.
// It means that lock MUST be acquired in memtable
// before calling those below methods.
class SkipList {
public:
  explicit SkipList(int max_level = 16);

  ~SkipList();

  // Copy constructor/assignment
  SkipList(const SkipList &) = delete;
  SkipList &operator=(const SkipList &) = delete;

  // Move constructor/assignment
  SkipList(SkipList &&) = default;
  SkipList &operator=(SkipList &&) = default;

  // Create SkipList iterator
  void CreateSkipListIterator();

  std::vector<std::pair<std::string, bool>>
  BatchDelete(std::span<std::string_view> keys, TxnId txn_id);

  bool Delete(std::string_view key, TxnId txn_id);

  // TODO(namnh) : shoud we use std::string?
  std::vector<std::pair<std::string, GetStatus>>
  BatchGet(std::span<std::string_view> keys, TxnId txn_id);

  GetStatus Get(std::string_view key, TxnId txn_id);

  std::vector<std::string> GetAllPrefixes(std::string_view key, TxnId txn_id);

  void BatchPut(std::span<std::pair<std::string_view, std::string_view>> pairs,
                TxnId txn_id);

  // Insert new key and value.
  // If key existed, update new value.
  void Put(std::string_view key, std::string_view value, TxnId txn_id);

  // TODO(namnh) : check type
  size_t GetCurrentSize();

  // Return random number of levels that a node is inserted
  int GetRandomLevel();

  // For debugging
  void PrintSkipList();

  friend class SkipListIterator;

private:
  void Put_(std::string_view key, std:optional<std::string_view> value,
            TxnId txn_id, ValueType value_type);

  // Get node at current 0 whose value is less than key.
  // Also, if the operation is PUT or DELETE, each node whose key < key needed
  // to find at each level needed to be found and be added into "updates" list
  std::shared_ptr<SkipListNode> FindLowerBoundNode(
      std::string_view key,
      std::vector<std::shared_ptr<SkipListNode>> *updates = nullptr) const;

  // adaptive number of current levels
  int current_level_;

  // TODO(namnh) Change when support config
  uint8_t max_level_;

  std::mt19937 gen_;

  std::uniform_int_distribution<> dist_level_;

  std::shared_ptr<SkipListNode> head_;

  size_t current_size_; // bytes unit

  // For testing mem leak
  std::shared_ptr<SkipListNode> deleted_node_;
};

} // namespace db

} // namespace kvs

#endif // DB_SKIPLIST_H
