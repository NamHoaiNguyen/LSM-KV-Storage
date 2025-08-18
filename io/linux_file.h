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
  LinuxAccessFile(std::string &&file_Name);

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

  ssize_t Write(DynamicBuffer &&buffer) override;

  ssize_t Write(std::span<const Byte> buffer) override;

private:
  ssize_t Write_(const char *buffer, size_t size);

  std::string file_name_;

  Fd fd_; // file descriptor

  std::unique_ptr<Buffer> buffer_;
};
} // namespace kvs

#endif // IO_LINUX_FILE_H