#ifndef IO_BASE_FILE_H
#define IO_BASE_FILE_H

#include "common/macros.h"

// libC++
#include <span>

// libC
#include <sys/types.h>

namespace kvs {

class Buffer;

class AccessFile {
public:
  virtual ~AccessFile() {}

  virtual bool Close() = 0;

  // Persist data from memory to disk
  virtual void Flush() = 0;

  virtual bool Open() = 0;

  virtual ssize_t Read() = 0;

  //  Append new data at offset
  virtual ssize_t Append(DynamicBuffer &&buffer, uint64_t offset) = 0;

  //  Append new data at offset
  virtual ssize_t Append(std::span<const Byte> buffer, uint64_t offset) = 0;
};
} // namespace kvs

#endif // IO_BASE_FILE_H