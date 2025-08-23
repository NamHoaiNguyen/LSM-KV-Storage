#ifndef IO_LINUX_FILE_H
#define IO_LINUX_FILE_H

#include "io/base_file.h"
#include "io/buffer.h"

#include <memory>
#include <string>

// C headers
#include <sys/types.h>

namespace kvs {

class Buffer;

class LinuxAccessFile : public AccessFile {
public:
  LinuxAccessFile(std::string &&filename);

  ~LinuxAccessFile();

  // No copy allowed
  LinuxAccessFile(const LinuxAccessFile &) = delete;
  LinuxAccessFile &operator=(LinuxAccessFile &) = delete;

  // Move constructor/assignment
  LinuxAccessFile(LinuxAccessFile &&) = default;
  LinuxAccessFile &operator=(LinuxAccessFile &&) = default;

  bool Close() override;

  void Flush() override;

  bool Open() override;

  ssize_t Read() override;

  ssize_t Append(DynamicBuffer &&buffer, uint64_t offset) override;

  // append data from buffer starting from offset
  ssize_t Append(std::span<const Byte> buffer, uint64_t offset) override;

private:
  ssize_t Append_(const uint8_t *buffer, size_t size, uint64_t offset);

  std::string filename_;

  // file descriptor
  Fd fd_;

  std::unique_ptr<Buffer> buffer_;
};
} // namespace kvs

#endif // IO_LINUX_FILE_H