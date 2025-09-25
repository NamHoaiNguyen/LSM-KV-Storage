#include "skiplist_node.h"

namespace kvs {

namespace db {

SkipListNode::SkipListNode(std::string_view key,
                           std::optional<std::string_view> value, TxnId txn_id,
                           int num_level, ValueType value_type)
    : key_(key),
      value_(value ? std::make_optional<std::string>(*value) : std::nullopt),
      txn_id_(txn_id), num_level_(num_level), forward_(num_level, nullptr),
      value_type_(value_type),
      backward_(num_level, std::weak_ptr<SkipListNode>()) {}

bool SkipListNode::operator==(const SkipListNode &other) {
  // TODO(namnh) : implement
  return false;
}

} // namespace db

} // namespace kvs