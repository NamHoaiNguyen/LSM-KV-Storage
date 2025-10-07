#ifndef COMMON_LRU_CACHE_H
#define COMMON_LRU_CACHE_H

#include "common/macros.h"
#include "sstable/lru_item.h"

// libC++
#include <atomic>
#include <cassert>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace kvs {

namespace sstable {

template <typename Key, typename Value> class LRUCache {
public:
  explicit LRUCache(int capacity);

  ~LRUCache() = default;

  // No copy allowed
  LRUCache(const LRUCache &) = delete;
  LRUCache &operator=(LRUCache &) = delete;

  // Move constructor/assignment
  LRUCache(LRUCache &&) = default;
  LRUCache &operator=(LRUCache &&) = default;

  const LRUItem<Key, Value> *Get(Key key);

  // const LRUItem<Key, Value> *InsertThenGetItem(Key key, Value &&data);
  const LRUItem<Key, Value> *InsertThenGetItem(Key key,
                                               std::unique_ptr<Value> data);

private:
  // NOT THREAD-SAFE. Lock MUST be acquired before calling this method
  void Evict();

  // Add item that refcount = 0 to free_list_
  void Release(Key key);

  std::unordered_map<Key, std::unique_ptr<LRUItem<Key, Value>>> cache_;

  std::list<Key> free_list_;

  int capacity_;

  mutable std::shared_mutex mutex_;
};

template <typename Key, typename Value>
LRUCache<Key, Value>::LRUCache(int capacity) : capacity_(capacity) {}

template <typename Key, typename Value>
const LRUItem<Key, Value> *LRUCache<Key, Value>::Get(Key key) {
  std::shared_lock rlock(mutex_);

  auto iterator = cache_.find(key);
  if (iterator == cache_.end()) {
    return nullptr;
  }

  // Increase object's ref count
  iterator->second->ref_count_.fetch_add(1);

  // return iterator->second.get();
  return nullptr;
}

template <typename Key, typename Value>
const LRUItem<Key, Value> *
LRUCache<Key, Value>::InsertThenGetItem(Key key, std::unique_ptr<Value> value) {
  std::scoped_lock rwlock(mutex_);
  auto lru_item =
      std::make_unique<LRUItem<Key, Value>>(key, std::move(value), this);
  bool success = false;

  if (cache_.size() >= capacity_) {
    Evict();
  }

  success = cache_.insert({key, std::move(lru_item)}).second;
  auto iterator = cache_.find(key);
  if (success) {
    // It meant that object hadn't existed in cache before
    // Get object data after inserted
    const LRUItem<Key, Value> *lru_item_immu = iterator->second.get();
    assert(lru_item_immu);

    // Increase object's ref count
    lru_item_immu->ref_count_.fetch_add(1);
  }

  return iterator->second.get();
}

// NOT THREAD-SAFE. Lock MUST be acquired before calling this method
template <typename Key, typename Value> void LRUCache<Key, Value>::Evict() {
  if (free_list_.empty()) {
    // Can't find any victim, just return
    return;
  }

  Key key = free_list_.front();
  // Remove least-recently used from cache
  cache_.erase(key);

  // Remove least-recently used from free list
  free_list_.pop_front();
}

template <typename Key, typename Value>
void LRUCache<Key, Value>::Release(Key key) {
  free_list_.insert_back(key);
}

} // namespace sstable

} // namespace kvs

#endif // COMMON_LRU_CACHE_H