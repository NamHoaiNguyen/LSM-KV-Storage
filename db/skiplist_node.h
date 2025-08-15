#ifndef DB_SKIPLIST_NODE_H
#define DB_SKIPLIST_NODE_H

#include "common/macros.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace kvs {

class SkipListIter;
class SkipList;
class SkipListNode : public std::enable_shared_from_this<SkipListNode> {
public:
  SkipListNode(std::string_view key, std::string_view value, int num_level);

  ~SkipListNode();

  bool operator==(const SkipListNode &other);

  friend class SkipList;
  friend class SkipListIterator;

private:
  std::string key_;

  std::string value_;

  // TODO(namnh) : recheck
  // TxnId txn_id_;

  // Number of level that this node appears
  int num_level_;

  // Travel from high level to low level
  std::vector<std::shared_ptr<SkipListNode>> forward_;

  // Use weak_ptr to avoid circular
  std::vector<std::weak_ptr<SkipListNode>> backward_;
};

} // namespace kvs

#endif // DB_SKIPLIST_NODE_H