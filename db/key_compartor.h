#ifndef DB_KEY_COMPARATOR_H
#define DB_KEY_COMPARATOR_H

#include "common/macros.h"

#include <memory>
#include <vector>

namespace kvs {

class KeyComparator {
  public:
    KeyComparator();

    ~KeyComparator();

    bool KeyComparator()(const std::string& a, const std::string& b);

  private:
    // TODO(change when support config)
    uint8_t levels{8};

    std::vector<std::shared_ptr>> SkipListNode;
};

} // namespace kvs

#endif // DB_KEY_COMPARATOR_H