#include "skiplist_node.h"

namespace kvs {

namespace db {

SkipListNode::SkipListNode(std::string_view key, std::string_view value,
                           int num_level)
    : key_(key), value_(value), num_level_(num_level),
      forward_(num_level, nullptr), value_type_(ValueType::PUT),
      backward_(num_level, std::weak_ptr<SkipListNode>()) {}

SkipListNode::~SkipListNode() = default;

bool SkipListNode::operator==(const SkipListNode &other) {
  // TODO(namnh) : implement
  return false;
}

} // namespace db

} // namespace kvs