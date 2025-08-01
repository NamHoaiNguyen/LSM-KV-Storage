#ifndef DB_SKIPLIST_H
#define DB_SKIPLIST_H

#include "common/macros.h"

#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace kvs {

class SkipListNode;

// ALL methods in this class ARE NOT THREAD-SAFE.
// It means that lock MUST be acquired in memtable
// before calling those below methods.
// Design concurrent-skiplist in future.

class SkipList {
public:
  SkipList();

  ~SkipList();

  // Copy constructor/assignment
  SkipList(const SkipList &) = delete;
  SkipList &operator=(const SkipList &) = delete;

  // Move constructor/assignment
  SkipList(SkipList &&) = default;
  SkipList &operator=(SkipList &&) = default;

  std::optional<std::string> Get(std::string_view key, TxnId txn_id);

  bool Delete(std::string_view key, TxnId txn_id);

  // Insert new key and value.
  // If key existed, update new value.
  void Put(std::string_view key, std::string_view value, TxnId txn_id);

  // TODO(namnh) : check type
  size_t GetCurrentSize();

  int GetRandomLevel();

  // Return random number of levels that a node is inserted

  // SkipListIterator begin();

  // SkipListIterator end();

  // For debugging
  void PrintSkipList();

  int CheckNodeRefCount();

private:
  std::vector<std::shared_ptr<SkipListNode>>
  FindNodeLessThan(std::string_view key);

  // adaptive number of current levels
  int current_level_;

  // TODO(namnh) Change when support config
  uint8_t max_level_;

  std::mt19937 gen_;

  std::uniform_int_distribution<> dist_level_;

  // TODO(namnh) : Can we use unique_ptr ?
  std::shared_ptr<SkipListNode> head_;

  size_t current_size_; // bytes unit

  // For testing mem leak
  std::shared_ptr<SkipListNode> deleted_node_;
};

} // namespace kvs

#endif // DB_SKIPLIST_H
