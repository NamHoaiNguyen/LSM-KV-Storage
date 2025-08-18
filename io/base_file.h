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

  virtual ssize_t Write(DynamicBuffer &&buffer) = 0;

  virtual ssize_t Write(std::span<const Byte> buffer) = 0;
};
} // namespace kvs

#endif // IO_BASE_FILE_H