#ifndef DB_SKIPLIST_H
#define DB_SKIPLIST_H

#include "common/macros.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace kvs {

class SkipListNode;

// ALL methods in this class ARE NOT THREAD-SAFE.
// It means that lock MUST be acquired in memtable
// before calling those below methods.
// Design concurrent-skiplist in future.

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

    std::optional<std::string> Get(std::string_view key);

    void Delete(std::string_view key);

    bool Put(std::string_view key, std::string_view value);

    void Update(std::string_view key, std::string_view value);

    // TODO(namnh) : check type
    size_t GetCurrentSize();

  private:
    void FindGreaterOrEqual(std::string_view key);

    // TODO(change when support config)
    uint8_t current_level_;

    uint8_t max_level_;

    std::shared_ptr<SkipListNode<std::string>> SkipListNode;

    size_t size_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H