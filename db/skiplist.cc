#include "db/skiplist.h"

#include "db/skiplist_node.h"

#include <cassert>

namespace {

// TODO(config instead of hard-fix)
constexpr int MAX_LEVEL = 8;

} // anonymous namespace

namespace kvs {

SkipList::SkipList()
    : current_level_(1), max_level_(MAX_LEVEL),
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

// TODO(namnh) : update when transaction is implemented.
std::optional<std::string> SkipList::Get(std::string_view key, TxnId txn_id) {
  std::shared_ptr<SkipListNode> current = head_;
  for (int level = current_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
  }

  // Move to next node in level 0
  current = current->forward_[0];

  if (current && current->key_ == key) {
    return std::make_optional<std::string>(std::string(current->value_));
  }

  return std::nullopt;
}

// TODO(namnh) : update when transaction is implemented.
void SkipList::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  int new_level = GetRandomLevel();

  // Each element in updates is a pointer pointing node whose key is
  // largest but less than key.
  std::vector<std::shared_ptr<SkipListNode>> updates(max_level_, nullptr);
  auto new_node = std::make_shared<SkipListNode>(key, value, new_level);

  std::shared_ptr<SkipListNode> current = head_;
  for (int level = current_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
    updates[level] = current;
  }

  // current->forward_[0] is the elem that we want to lookup
  current = current->forward_[0];
  if (current && current->key_ == key) {
    // If key which is being found exists, just update value
    current_size_ += value.size() - current->value_.size();
    current->value_ = value;

    return;
  }

  // Update new size of skiplist
  current_size_ += key.size() + value.size();

  if (new_level > current_level_) {
    // We need to rechange size.
    for (int level = current_level_; level < new_level; ++level) {
      updates[level] = head_;
      // We need to rechange size.
      updates[level]->forward_.resize(new_level, nullptr);
      updates[level]->backward_.resize(new_level);
    }
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

  // Update skip list's current level
  current_level_ = new_level;
}

bool SkipList::Delete(std::string_view key, TxnId txn_id) {
  // Each element in updates is a pointer pointing node whose key is
  // largest but less than key.
  std::vector<std::shared_ptr<SkipListNode>> updates(current_level_, nullptr);
  std::shared_ptr<SkipListNode> current = head_;
  for (int level = current_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
    updates[level] = current;
  }

  // current->forward_[0] is the node that we want to lookup
  current = current->forward_[0];
  if (!current) {
    return false;
  }

  if (current->key_ != key) {
    // Key doesn't exist
    return false;
  }

  for (int level = 0; level < current_level_; ++level) {
    if (updates[level]->forward_[level]->key_ != key) {
      // No need to go to higher level
      break;
    }
    updates[level]->forward_[level] = current->forward_[level];
    if (current->forward_[level]) {
      current->forward_[level]->backward_[level] = updates[level];
    }
  }

  // Update size of skiplist
  current_size_ -= key.size() + current->value_.size();

  // Update current_level_
  while (current_level_ > 1 && head_->forward_[current_level_ - 1] == nullptr) {
    current_level_--;
  }

  // For testing memleak.
  // Up until this point, there is no other nodes referencing to current node
  // (node should be deleted).
  deleted_node_ = std::move(current);

  return true;
}

size_t SkipList::GetCurrentSize() { return current_size_; }

int SkipList::CheckNodeRefCount() {
  int ref_count = deleted_node_.use_count();
  // Reset mem of deleted node
  deleted_node_.reset();

  // ref_count SHOULD be = 1.
  return ref_count;
}

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