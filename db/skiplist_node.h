#ifndef DB_SKIPLIST_H
#define DB_SKIPLIST_H

#include "common/macros.h"

#include <memory>
#include <string>
#include <vector>

namespace kvs {

// TODO(namnh) : Currently, we only support string type.
// template<typename Type, typename Comparator>
class SkipListNode {
  public:
    SkipListNode();

    ~SkipListNode();

    bool operator==(const SkipListNode& other);

    void GetKey(std::string_view key);
    void PutKey(std::string_view key, std::string_view value);

  private:
    std::string key_;

    std::string value_;

    // Travel from high level to low level
    std::vector<std::shared_ptr<SkipListNode<std::string>> forward_;

    // Travel from low level to high level
    std::vector<std::shared_ptr<SkipListNode<std::string>> backward_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H