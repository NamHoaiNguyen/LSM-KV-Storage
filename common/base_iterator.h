#ifndef COMMON_BASE_ITERATOR_H
#define COMMON_BASE_ITERATOR_H

#include <string_view>

namespace kvs {

class BaseIterator {
public:
  BaseIterator() {}

  virtual ~BaseIterator() {}

  // No copying allowed
  BaseIterator(const BaseIterator &) = delete;
  void operator=(const BaseIterator &) = delete;

  virtual bool IsValid() = 0;

  virtual std::string_view GetKey() = 0;

  virtual std::string_view GetValue() = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual void Seek(std::string_view) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;
};

} // namespace kvs

#endif // COMMON_BASE_ITERATOR_H