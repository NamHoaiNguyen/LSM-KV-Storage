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

LinuxAccessFile::LinuxAccessFile(std::string &&file_name)
    : file_name_(std::move(file_name)), buffer_(std::make_unique<Buffer>()) {}

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
  fd_ = ::open(file_name_.c_str(), O_CREAT | O_TRUNC | O_WRONLY);
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

// ssize_t LinuxAccessFile::Write(DynamicBuffer &&buffer) {
//   std::vector<Byte> tmp_buffer = std::move(buffer);
//   DynamicBuffer &file_buffer = buffer_->GetBuffer();

//   ssize_t total_bytes = 0;
//   size_t tmp_buffer_len = tmp_buffer.size();

//   for (size_t i = 0; i < tmp_buffer_len / kDefaultBufferSize; i++) {
//     file_buffer.insert(
//         file_buffer.end(),
//         std::make_move_iterator(tmp_buffer.begin() + i * kDefaultBufferSize),
//         std::make_move_iterator(tmp_buffer.begin() +
//                                 (i + 1) * kDefaultBufferSize));
//     const char *buff = reinterpret_cast<const char *>(file_buffer.data());
//     size_t buff_len = file_buffer.size();

//     while (buff_len > 0) {
//       ssize_t bytes_written = ::write(fd_, buff, buff_len);
//       if (bytes_written < 0) {
//         if (errno == EINTR) {
//           continue; // Retry
//         }
//         return -1;
//       }
//       buff += bytes_written;
//       buff_len -= bytes_written;
//       total_bytes += bytes_written
//     }
//   }

//   return total_bytes;
// }

ssize_t LinuxAccessFile::Write(DynamicBuffer &&buffer) {
  std::vector<Byte> tmp_buffer = std::move(buffer);
  const char *buff = reinterpret_cast<const char *>(tmp_buffer.data());
  size_t size = tmp_buffer.size();

  return Write_(buff, size);
}

ssize_t LinuxAccessFile::Write(std::span<const Byte> buffer) {
  const char *buff = reinterpret_cast<const char *>(buffer.data());
  size_t size = buffer.size();

  return Write_(buff, size);
}

ssize_t LinuxAccessFile::Write_(const char *buffer, size_t size) {
  ssize_t total_bytes = 0;

  while (size > 0) {
    ssize_t bytes_written = ::write(fd_, buffer, size);
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