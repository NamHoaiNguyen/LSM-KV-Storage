#include "db/skiplist_node.h"

namespace kvs {

template<typenamne Type>
SkipListNode::SkipListNode() = default;

template<typenamne Type>
SkipListNode::~SkipListNode() = default;

template<typenamne Type>
bool SkipListNode::operator==(const SkipListNode<Type>& other) {
  // TODO(namnh) : implement
  return false;
}

} // namespace kvs