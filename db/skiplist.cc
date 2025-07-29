#include "db/skiplist.h"

namespace {

// TODO(config instead of hard-fix)
constepxr int max_level = 16;

int GetRandomLevel() {
  int level{0};

  return level
}

} // anonymous namespace

namespace kvs {

SkipList::SkipList() : current_level_(1), max_level_(max_level) {}

SkipList::~SkipList() = default;

std::optional<std::string> SkipList::Get(std::string_view key) {
  

  return std::nullopt;
}

bool SkipList::Put(std::string_view key, std::string_view value) {
  if (Get(key).has_value()) {
    // If node found, return.
    return false;
  }



  return true;
}

size_t SkipList::GetSize() {
  return size_;
}

} // namespace kvs