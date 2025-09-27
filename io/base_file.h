#ifndef IO_BASE_FILE_H
#define IO_BASE_FILE_H

#include "common/macros.h"

// libC++
#include <span>

// libC
#include <sys/types.h>

namespace kvs {

namespace io {

class Buffer;

class WriteOnlyFile {
public:
  virtual ~WriteOnlyFile() {}

  virtual bool Close() = 0;

  // Persist data from memory to disk
  virtual bool Flush() = 0;

  virtual bool Open() = 0;

  //  Append new data at offset
  virtual ssize_t Append(DynamicBuffer &&buffer, uint64_t offset) = 0;

  //  Append new data at offset
  virtual ssize_t Append(std::span<const Byte> buffer, uint64_t offset) = 0;

  virtual ssize_t AppendAtLast(std::span<const Byte> buffer) = 0;
};

class ReadOnlyFile {
public:
  virtual ~ReadOnlyFile() {};

  virtual bool Close() = 0;

  virtual bool Open() = 0;

  virtual ssize_t RandomRead(std::span<Byte> buffer, uint64_t offset) = 0;
};

} // namespace io

} // namespace kvs

#endif // IO_BASE_FILE_H