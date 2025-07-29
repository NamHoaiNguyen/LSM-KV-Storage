#include "db/skiplist.h"

namespace {

// TODO(config instead of hard-fix)
constepxr int MAX_LEVEL = 8;

int GetRandomLevel() {
  int level{0};

  while (dist_level(gen_) && level < max_level_) {
    level++;
  }

  return level;
}

} // anonymous namespace

namespace kvs {

SkipList::SkipList()
    : current_level_(1), max_level_(MAX_LEVEL),
      gen_(std::mt19937(std::random_device()())) {}

SkipList::~SkipList() = default;

std::optional<std::string> SkipList::Get(std::string_view key) {

  return std::nullopt;
}

bool SkipList::Put(std::string_view key, std::string_view value) {
  if (Get(key).has_value()) {
    // If node found, return.
    return false;
  }

  auto new_node = std::make_shared<SkipListNode>(key, value, GetRandomLevel());
  for (int level = max_level_ - 1; level >= 0; level--) {
  }

  return true;
}

size_t SkipList::GetSize() { return size_; }

void SkipList::PrintSkipList() {
  for (int level = 0; i < current_level_; i++) {
    std::cout << "Level " << level << ": ";
    std::shared_ptr<SkipListNode> current = head_->forward_[level];
    while (current) {
      std::cout << current->key << "," << current->value;
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