#ifndef COMMON_LRU_CACHE_ITEM_H
#define COMMON_LRU_CACHE_ITEM_H

#include "common/macros.h"

// libC++
#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace kvs {

namespace sstable {

template <typename Key, typename Value> class LRUCache;

template <typename Key, typename Value> struct LRUItem {
public:
  LRUItem(Key key, std::unique_ptr<Value> value,
          const LRUCache<Key, Value> *lru);

  ~LRUItem() = default;

  // No copy allowed
  LRUItem(const LRUItem &) = delete;
  LRUItem &operator=(LRUItem &) = delete;

  // Move constructor/assignment
  LRUItem(LRUItem &&) = default;
  LRUItem &operator=(LRUItem &&) = default;

  template <typename KeyType, typename ValueType> friend class LRUCache;

  // It is YOUR(ACTUALLY MY) responsibility to call it each time you finish
  void Unref();

  const Value *GetValue() const;

private:
  mutable std::atomic<uint64_t> ref_count_{0};

  Key key_;

  std::unique_ptr<Value> value_;

  const LRUCache<Key, Value> *lru_;
};

template <typename Key, typename Value>
LRUItem<Key, Value>::LRUItem(Key key, std::unique_ptr<Value> value,
                             const LRUCache<Key, Value> *lru)
    : key_(key), value_(std::move(value)), lru_(lru) {}

template <typename Key, typename Value> void LRUItem<Key, Value>::Unref() {
  if (ref_count_.fetch_sub(1) == 1) {
    lru_->Release(key_);
  }
}

template <typename Key, typename Value>
const Value *LRUItem<Key, Value>::GetValue() const {
  return value_.get();
}

} // namespace sstable

} // namespace kvs

#endif // COMMON_LRU_CACHE_ITEM_H