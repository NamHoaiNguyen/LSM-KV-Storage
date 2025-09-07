#include "io/linux_file.h"

#include "common/macros.h"
#include "io/buffer.h"

// C++ libs
#include <cerrno>
#include <cstring>
#include <iostream>

// C libs
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace kvs {

namespace io {

// ==========================Start LinuxWriteOnlyFile==========================

LinuxWriteOnlyFile::LinuxWriteOnlyFile(std::string_view filename)
    : filename_(filename) {}

LinuxWriteOnlyFile::~LinuxWriteOnlyFile() { Close(); }

bool LinuxWriteOnlyFile::Close() {
  if (::close(fd_) == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }

  return true;
}

bool LinuxWriteOnlyFile::Flush() {
  if (::fsync(fd_) < 0) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }

  return true;
}

bool LinuxWriteOnlyFile::Open() {
  // TODO(namnh) : Recheck
  chmod(filename_.c_str(), 0644);

  fd_ = ::open(filename_.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
  if (fd_ == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }
  return true;
}

// append
ssize_t LinuxWriteOnlyFile::Append(DynamicBuffer &&buffer, uint64_t offset) {
  return Append_(buffer.data(), buffer.size(), offset);
}

// append
ssize_t LinuxWriteOnlyFile::Append(std::span<const Byte> buffer,
                                   uint64_t offset) {
  return Append_(buffer.data(), buffer.size(), offset);
}

ssize_t LinuxWriteOnlyFile::Append_(const uint8_t *buffer, size_t size,
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

// ===========================End LinuxWriteOnlyFile===========================

// ===========================Start LinuxReadOnlyFile===========================

LinuxReadOnlyFile::LinuxReadOnlyFile(std::string_view filename)
    : filename_(std::move(filename)), buffer_(std::make_unique<Buffer>()) {}

LinuxReadOnlyFile::~LinuxReadOnlyFile() { Close(); }

bool LinuxReadOnlyFile::Open() {
  fd_ = ::open(filename_.c_str(), O_RDONLY);
  if (fd_ == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }
  return true;
}

bool LinuxReadOnlyFile::Close() {
  if (::close(fd_) == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }

  return true;
}

ssize_t LinuxReadOnlyFile::RandomRead(uint64_t offset, size_t size) {
  size = std::min(size, kDefaultBufferSize);

  ssize_t read_bytes = ::pread(fd_, buffer_->GetBuffer().data(), size,
                               static_cast<off64_t>(offset));
  if (read_bytes < 0) {
    return -1;
  }

  return read_bytes;
}

Buffer *LinuxReadOnlyFile::GetBuffer() { return buffer_.get(); }

// ===========================End LinuxReadOnlyFile===========================

} // namespace io

} // namespace kvs