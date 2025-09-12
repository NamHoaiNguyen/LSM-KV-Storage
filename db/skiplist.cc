#include "db/skiplist.h"

#include "db/skiplist_node.h"

namespace kvs {

namespace db {

SkipList::SkipList(int max_level)
    : current_level_(1), max_level_(max_level),
      gen_(std::mt19937(std::random_device()())), current_size_(0),
      dist_level_(std::uniform_int_distribution<>(0, 1)),
      head_(std::make_shared<SkipListNode>(
          "" /*key*/, "" /*value*/, current_level_, ValueType::NOT_FOUND)) {}

// Return random number of levels that a node is inserted
int SkipList::GetRandomLevel() {
  int level = 1;

  while (dist_level_(gen_) && level < max_level_) {
    level++;
  }

  return level;
}

void SkipList::BatchDelete(std::span<std::string_view> keys, TxnId txn_id) {
  for (std::string_view key : keys) {
    Delete(key, txn_id);
  }
}

std::vector<std::pair<std::string, GetStatus>>
SkipList::BatchGet(std::span<std::string_view> keys, TxnId txn_id) {
  std::vector<std::pair<std::string, GetStatus>> values;
  std::shared_ptr<SkipListNode> current{nullptr};

  GetStatus status;
  for (std::string_view key : keys) {
    status = Get(key, txn_id);
    values.push_back({std::string{key}, std::move(status)});
  }

  return values;
}

// TODO(namnh) : update when transaction is implemented.
GetStatus SkipList::Get(std::string_view key, TxnId txn_id) {
  std::shared_ptr<SkipListNode> current = FindLowerBoundNode(key);
  GetStatus status;

  if (!current || current->key_ != key) {
    status.type = ValueType::NOT_FOUND;
    status.value = std::nullopt;

    return status;
  }

  status.type = current->value_type_;
  status.value = (current->value_type_ == ValueType::PUT) ? (current->value_)
                                                          : std::nullopt;
  return status;
}

std::vector<std::optional<std::string>>
SkipList::GetAllPrefixes(std::string_view key, TxnId txn_id) {
  std::vector<std::optional<std::string>> values;
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
  Put_(key, value, txn_id, ValueType::PUT /*ValueType*/);
}

void SkipList::Delete(std::string_view key, TxnId txn_id) {
  // Each element in updates is a pointer pointing node whose key is
  // largest but less than key.
  Put_(key, std::nullopt /*value*/, txn_id, ValueType::DELETED);
}

void SkipList::Put_(std::string_view key, std::optional<std::string_view> value,
                    TxnId txn_id, ValueType value_type) {
  std::vector<std::shared_ptr<SkipListNode>> updates(max_level_, nullptr);
  std::shared_ptr<SkipListNode> current = FindLowerBoundNode(key, &updates);

  if (value_type == ValueType::PUT) {
    if (current && current->key_ == key) {
      // If key which is being found exists, just update value
      current_size_ += value.value().size() - current->value_.value().size();
      current->value_ = (value) ? std::make_optional<std::string>(value.value())
                                : std::nullopt;
      return;
    }
    // Update new size of skiplist
    current_size_ += key.size() + value.value().size();
  } else if (value_type == ValueType::DELETED) {
    // Delete operatios is "put" op without value
    current_size_ += key.size();
  }

  int new_level = GetRandomLevel();
  auto new_node =
      std::make_shared<SkipListNode>(key, value, new_level, value_type);
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
      if (current->value_) {
        std::cout << "( " << static_cast<int>(current->value_type_) << " "
                  << current->key_ << "," << (current->value_).value() << " )";
      } else {
        std::cout << "( " << static_cast<int>(current->value_type_) << " "
                  << current->key_ << " )";
      }
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

} // namespace db

} // namespace kvs