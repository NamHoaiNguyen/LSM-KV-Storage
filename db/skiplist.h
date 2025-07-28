#ifndef DB_SKIPLIST_H
#define DB_SKIPLIST_H

#include "common/macros.h"

#include <memory>
#include <string_view>
#include <vector>

namespace kvs {

class SkipListNode;

// template<typename Type>
class SkipList {
  public:
    SkipList();

    ~SkipList();

    // Copy constructor/assignment
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // Move constructor/assignment
    SkipList(SkipList&&) = default;
    SkipList& operator=(SkipList&&) = default;

    void Put(std::string_view key, std::string_view value);

    void Get(std::string_view key);

  private:
    // TODO(change when support config)
    uint8_t levels;

    std::shared_ptr<SkipListNode<std::string>> SkipListNode;
};

} // namespace kvs

#endif // DB_MEMTABLE_H