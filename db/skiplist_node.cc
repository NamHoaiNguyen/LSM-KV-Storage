#include "db/skiplist_node.h"

namespace kvs {

SkipListNode::SkipListNode(std::string_view key, std::string_view value,
                           int num_level)
    : key_(key), value_(value), num_level_(num_level) {}

SkipListNode::~SkipListNode() = default;

bool SkipListNode::operator==(const SkipListNode &other) {
  // TODO(namnh) : implement
  return false;
}

void GetKey(std::string_view key)

} // namespace kvs