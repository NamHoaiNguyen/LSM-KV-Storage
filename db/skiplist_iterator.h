#ifndef DB_SKIPLIST_ITERATOR_H
#define DB_SKIPLIST_ITERATOR_H

#include "common/base_iterator.h"

#include <memory>
#include <shared_mutex>
#include <string_view>

namespace kvs {

class SkipList;
class SkipListNode;

class SkipListIterator : public BaseIterator {
public:
  SkipListIterator(const SkipList *skiplist);

  ~SkipListIterator() override;

  std::string_view GetKey() override;

  std::string_view GetValue() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  // Return the node which have smallest key that is
  // greater than or equal to key
  virtual void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  // NOTE: DONT free this pointer.
  // This pointer is taken by unique_ptr GET API .It will be auto-released.
  // Releasing memory can cause undefined behaviour.
  const SkipList *skiplist_;

  std::shared_ptr<const SkipListNode> node_;
};

} // namespace kvs

#endif // DB_SKIPLIST_ITERATOR_H