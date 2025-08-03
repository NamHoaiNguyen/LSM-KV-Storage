#ifndef DB_SKIPLIST_ITERATOR_H
#define DB_SKIPLIST_ITERATOR_H

#include "db/base_iterator.h"

namespace kvs {

#include <memory>
#include <shared_mutex>

// class SkipListIterator {
// public:
//   ~SkipListIterator() override;

//   bool operator==(const BaseIterator &other) override;

//   bool operator!=(const BaseIterator &other) override;

//   BaseIterator &operator++() override;

// private:
//   std::shared_mutex mutex_;
// };

} // namespace kvs

#endif // DB_SKIPLIST_ITERATOR_H