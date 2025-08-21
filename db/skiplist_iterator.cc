#include "skiplist_iterator.h"

#include "skiplist.h"
#include "skiplist_node.h"

namespace kvs {

SkipListIterator::SkipListIterator(const SkipList *skiplist)
    : skiplist_(skiplist) {}

SkipListIterator::~SkipListIterator() = default;

std::string_view SkipListIterator::GetKey() { return node_->key_; }

std::string_view SkipListIterator::GetValue() { return node_->value_; }

bool SkipListIterator::IsValid() { return node_ != nullptr; }

void SkipListIterator::Next() { node_ = node_->forward_[0]; }

void SkipListIterator::Prev() { node_ = node_->backward_[0].lock(); }

void SkipListIterator::Seek(std::string_view key) {
  node_ = skiplist_->FindLowerBoundNode(key);
  if (node_ && node_->key_ == key) {
    return;
  }
  node_ = node_->forward_[0];
}

void SkipListIterator::SeekToFirst() { node_ = skiplist_->head_->forward_[0]; }

void SkipListIterator::SeekToLast() {
  node_ = skiplist_->head_->forward_[0];
  while (node_->forward_[0]) {
    node_ = node_->forward_[0];
  }
}

} // namespace kvs