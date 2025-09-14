#ifndef IO_LINUX_FILE_H
#define IO_LINUX_FILE_H

#include "io/base_file.h"
#include "io/buffer.h"

#include <memory>
#include <string>

// C headers
#include <sys/types.h>

namespace kvs {

namespace io {

class Buffer;

class LinuxWriteOnlyFile : public WriteOnlyFile {
public:
  explicit LinuxWriteOnlyFile(std::string_view filename);

  ~LinuxWriteOnlyFile();

  // No copy allowed
  LinuxWriteOnlyFile(const LinuxWriteOnlyFile &) = delete;
  LinuxWriteOnlyFile &operator=(LinuxWriteOnlyFile &) = delete;

  // Move constructor/assignment
  LinuxWriteOnlyFile(LinuxWriteOnlyFile &&) = default;
  LinuxWriteOnlyFile &operator=(LinuxWriteOnlyFile &&) = default;

  bool Open() override;

  bool Close() override;

  bool Flush() override;

  ssize_t Append(DynamicBuffer &&buffer, uint64_t offset) override;

  // append data from buffer starting from offset
  ssize_t Append(std::span<const Byte> buffer, uint64_t offset) override;

private:
  ssize_t Append_(const uint8_t *buffer, size_t size, uint64_t offset);

  std::string filename_;

  // file descriptor
  Fd fd_;
};

class LinuxReadOnlyFile : public ReadOnlyFile {
public:
  explicit LinuxReadOnlyFile(std::string_view filename);

  ~LinuxReadOnlyFile();

  bool Open() override;

  bool Close() override;

  ssize_t RandomRead(std::span<Byte> buffer, uint64_t offset) override;

private:
  std::string filename_;

  Fd fd_;
};

} // namespace io

} // namespace kvs

#endif // IO_LINUX_FILE_H