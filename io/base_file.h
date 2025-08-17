#ifndef IO_BASE_FILE_H
#define IO_BASE_FILE_H

#include <sys/types.h>

namespace kvs {
class AccessFile {
public:
  virtual ~AccessFile() {}

  virtual bool Close() = 0;

  // Persist data from memory to disk
  virtual void Flush() = 0;

  virtual void Open() = 0;

  virtual ssize_t Read() = 0;

  virtual ssize_t Write() = 0;
};
} // namespace kvs

#endif // IO_BASE_FILE_H