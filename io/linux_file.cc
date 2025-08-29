#include "io/linux_file.h"

#include "common/macros.h"
#include "io/buffer.h"

// C++ libs
#include <cerrno>
#include <cstring>
#include <iostream>

// C libs
#include <fcntl.h>
#include <unistd.h>

namespace kvs {

LinuxAccessFile::LinuxAccessFile(std::string &&filename)
    : filename_(std::move(filename)), buffer_(std::make_unique<Buffer>()) {}

LinuxAccessFile::~LinuxAccessFile() { Close(); }

bool LinuxAccessFile::Close() {
  if (::close(fd_) == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }

  return true;
}

void LinuxAccessFile::Flush() {
  if (::fsync(fd_) < 0) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return;
  }
}

bool LinuxAccessFile::Open() {
  fd_ = ::open(filename_.c_str(), O_CREAT | O_TRUNC | O_WRONLY);
  if (fd_ == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }
  return true;
}

ssize_t LinuxAccessFile::Read() {
  char *buff = reinterpret_cast<char *>(buffer_->GetBuffer().data());
  size_t buff_len = buffer_->GetBufferLength();

  return ::read(fd_, buff, buff_len);
}

// append
ssize_t LinuxAccessFile::Append(DynamicBuffer &&buffer, uint64_t offset) {
  return Append_(buffer.data(), buffer.size(), offset);
}

// append
ssize_t LinuxAccessFile::Append(std::span<const Byte> buffer, uint64_t offset) {
  return Append_(buffer.data(), buffer.size(), offset);
}

ssize_t LinuxAccessFile::Append_(const uint8_t *buffer, size_t size,
                                 uint64_t offset) {
  ssize_t total_bytes = 0;

  while (size > 0) {
    ssize_t bytes_written =
        ::pwrite64(fd_, buffer, size, static_cast<off64_t>(offset));
    if (bytes_written < 0) {
      if (errno == EINTR) {
        continue; // Retry
      }
      return -1;
    }
    buffer += bytes_written;
    size -= bytes_written;
    total_bytes += bytes_written;
  }

  return total_bytes;
}

} // namespace kvs