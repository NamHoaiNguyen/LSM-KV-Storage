#ifndef DB_SKIPLIST_H
#define DB_SKIPLIST_H

#include "common/macros.h"

#include <memory>
#include <string>
#include <vector>

namespace kvs {

claas SkipList;

// TODO(namnh) : Currently, we only support string type.
// template<typename Type, typename Comparator>
class SkipListNode {
public:
  SkipListNode(std::string_view key, std::string_view value, int num_level);

  ~SkipListNode();

  bool operator==(const SkipListNode &other);

  friend class SkipList;

private:
  std::string key_;

  std::string value_;

  // Number of level that this node appears
  int num_level_;

  // Travel from high level to low level
  std::vector < std::shared_ptr<SkipListNode> forward_;

  // Travel from low level to high level
  std::vector < std::shared_ptr<SkipListNode> backward_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H