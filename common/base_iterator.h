#ifndef COMMON_BASE_ITERATOR_H
#define COMMON_BASE_ITERATOR_H

#include "common/macros.h"
#include "db/status.h"

// libC++
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

  virtual ValueType GetType() = 0;

  virtual TxnId GetTransactionId() = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  virtual void Seek(std::string_view key) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;
};

} // namespace kvs

#endif // COMMON_BASE_ITERATOR_H