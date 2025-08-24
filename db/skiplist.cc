#include "db/skiplist.h"

#include "db/skiplist_node.h"

namespace kvs {

SkipList::SkipList(int max_level)
    : current_level_(1), max_level_(max_level),
      gen_(std::mt19937(std::random_device()())), current_size_(0),
      dist_level_(std::uniform_int_distribution<>(0, 1)),
      head_(std::make_shared<SkipListNode>("" /*key*/, "" /*value*/,
                                           current_level_)) {}

SkipList::~SkipList() = default;

// Return random number of levels that a node is inserted
int SkipList::GetRandomLevel() {
  int level = 1;

  while (dist_level_(gen_) && level < max_level_) {
    level++;
  }

  return level;
}

std::vector<std::pair<std::string, bool>>
SkipList::BatchDelete(std::span<std::string_view> keys, TxnId txn_id) {
  std::vector<std::pair<std::string, bool>> res;
  for (std::string_view key : keys) {
    res.push_back({std::string(key), Delete(key, txn_id)});
  }

  return res;
}

bool SkipList::Delete(std::string_view key, TxnId txn_id) {
  // Each element in updates is a pointer pointing node whose key is
  // largest but less than key.
  std::vector<std::shared_ptr<SkipListNode>> updates(current_level_, nullptr);
  std::shared_ptr<SkipListNode> current = FindLowerBoundNode(key, &updates);
  if (!current) {
    return false;
  }

  if (current->key_ == key) {
    // If key which is being found exists, just update value type
    current->value_type_ = ValueType::DELETE;
    return true;
  }

  // Key not found
  return false;
}

std::vector<std::pair<std::string, std::optional<std::string>>>
SkipList::BatchGet(std::span<std::string_view> keys, TxnId txn_id) {
  std::vector<std::pair<std::string, std::optional<std::string>>> values;
  std::shared_ptr<SkipListNode> current{nullptr};

  std::optional<std::string> value;
  for (std::string_view key : keys) {
    value = Get(key, txn_id);
    values.push_back({std::string{key}, value});
  }

  return values;
}

// TODO(namnh) : update when transaction is implemented.
std::optional<std::string> SkipList::Get(std::string_view key, TxnId txn_id) {
  std::shared_ptr<SkipListNode> current = FindLowerBoundNode(key);
  if (current && current->key_ == key &&
      current->value_type_ != ValueType::DELETE) {
    return std::make_optional<std::string>(std::string(current->value_));
  }

  return std::nullopt;
}

std::vector<std::string> SkipList::GetAllPrefixes(std::string_view key,
                                                  TxnId txn_id) {
  std::vector<std::string> values;
  std::shared_ptr<SkipListNode> current = FindLowerBoundNode(key);

  // Traverse while key starts with prefix
  while (current && current->key_.starts_with(key)) {
    values.push_back(current->value_);
    current = current->forward_[0];
  }

  return values;
}

void SkipList::BatchPut(
    std::span<std::pair<std::string_view, std::string_view>> pairs,
    TxnId txn_id) {
  for (const auto &pair : pairs) {
    Put(pair.first /*key*/, pair.second /*value*/, txn_id);
  }
}

// TODO(namnh) : update when transaction is implemented.
void SkipList::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  std::vector<std::shared_ptr<SkipListNode>> updates(max_level_, nullptr);
  std::shared_ptr<SkipListNode> current = FindLowerBoundNode(key, &updates);
  if (current && current->key_ == key) {
    // If key which is being found exists, just update value
    current_size_ += value.size() - current->value_.size();
    current->value_ = value;

    return;
  }

  // Update new size of skiplist
  current_size_ += key.size() + value.size();

  int new_level = GetRandomLevel();
  auto new_node = std::make_shared<SkipListNode>(key, value, new_level);
  if (new_level > current_level_) {
    for (int level = current_level_; level < new_level; ++level) {
      updates[level] = head_;
      // We need to rechange size.
      updates[level]->forward_.resize(new_level, nullptr);
      updates[level]->backward_.resize(new_level);
    }
    current_level_ = new_level;
  }

  // Insert!!!
  for (int level = 0; level < new_level; ++level) {
    new_node->forward_[level] = updates[level]->forward_[level];
    if (new_node->forward_[level]) {
      new_node->forward_[level]->backward_[level] = new_node;
    }

    updates[level]->forward_[level] = new_node;
    new_node->backward_[level] = updates[level];
  }
}

std::shared_ptr<SkipListNode> SkipList::FindLowerBoundNode(
    std::string_view key,
    std::vector<std::shared_ptr<SkipListNode>> *updates) const {
  std::shared_ptr<SkipListNode> current = head_;

  for (int level = current_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
    if (updates) {
      (*updates)[level] = current;
    }
  }

  // Move to next node in level 0
  current = current->forward_[0];
  return current;
}

size_t SkipList::GetCurrentSize() { return current_size_; }

void SkipList::PrintSkipList() {
  for (int level = 0; level < current_level_; level++) {
    std::cout << "Level " << level << ": ";
    std::shared_ptr<SkipListNode> current = head_->forward_[level];
    while (current) {
      std::cout << "(" << current->key_ << "," << current->value_ << ")";
      current = current->forward_[level];
      if (current) {
        std::cout << " -> ";
      }
    }
    // New level.
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

} // namespace kvs