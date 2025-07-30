#include "db/skiplist.h"

#include "db/skiplist_node.h"

namespace {

// TODO(config instead of hard-fix)
constexpr int MAX_LEVEL = 8;

} // anonymous namespace

namespace kvs {

SkipList::SkipList()
    : current_level_(1), max_level_(MAX_LEVEL),
      gen_(std::mt19937(std::random_device()())), current_size_(0),
      head_(std::make_shared<SkipListNode>("" /*key*/, "" /*value*/,
                                           current_level_)) {}

SkipList::~SkipList() = default;

// Return random number of levels that a node is inserted
int SkipList::GetRandomLevel() {
  int level{0};

  while (dist_level_(gen_) && level < max_level_) {
    level++;
  }

  return level;
}

// TODO(namnh) : update when transaction is implemented.
std::optional<std::string> SkipList::Get(std::string_view key) {
  std::shared_ptr<SkipListNode> current = head_;
  for (int level = max_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
  }

  // Move to next node in level 0
  current = current->forward_[0];

  if (current && current->key_ == key) {
    return std::make_optional<std::string>(std::string(key));
  }

  return std::nullopt;
}

// TODO(namnh) : update when transaction is implemented.
bool SkipList::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  if (Get(key).has_value()) {
    // If node found, return.
    return false;
  }

  int new_level = GetRandomLevel();
  // Each element in updates is a pointer pointing node whose key is
  // largest but less than key.
  std::vector<std::shared_ptr<SkipListNode>> updates(max_level_, nullptr);
  auto new_node = std::make_shared<SkipListNode>(key, value, new_level);

  std::shared_ptr<SkipListNode> current = head_;
  for (int level = max_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
    updates[level] = current;
  }

  // ALL keys exists at level 0.
  // If key which is being found exists, just update value
  current = current->forward_[0];
  if (current->forward_[0] && current->forward_[0]->key_ == key) {
    current_size_ += value.size() - current->value_.size();
    current->value_ = value;
  } else {
    // Update new size of skiplist
    current_size_ += key.size() + value.size();
  }

  if (new_level > current_level_) {
    for (int level = current_level_; level < new_level; ++level) {
      updates[level] = head_;
    }
    // Update skip list's current level
    current_level_ = new_level;
  }

  // Insert!!!
  for (int level = 0; level < new_level; ++level) {
    new_node->forward_[level] = updates[level]->forward_[level];
    updates[level]->forward_[level] = updates[level];
  }

  return true;
}

void SkipList::Delete(std::string_view key) {}

std::vector<std::shared_ptr<SkipListNode>>
SkipList::FindNodeLessThan(std::string_view key) {
  std::vector<std::shared_ptr<SkipListNode>> prev_nodes(max_level_, nullptr);

  std::shared_ptr<SkipListNode> current = head_;
  for (int level = max_level_ - 1; level >= 0; --level) {
    while (current->forward_[level] && current->forward_[level]->key_ < key) {
      current = current->forward_[level];
    }
    prev_nodes[level] = current;
  }

  return prev_nodes;
}

size_t SkipList::GetCurrentSize() { return current_size_; }

void SkipList::PrintSkipList() {
  for (int level = 0; level < current_level_; level++) {
    std::cout << "Level " << level << ": ";
    std::shared_ptr<SkipListNode> current = head_->forward_[level];
    while (current) {
      std::cout << current->key_ << "," << current->value_;
      current = current->forward_[level];
      if (current) {
        std::cout << " ->";
      }
    }
    // New level.
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

} // namespace kvs