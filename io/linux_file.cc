#include "io/linux_file.h"

#include "buffer.h"

// C++ libs
#include <cerrno>
#include <cstring>
#include <iostream>

// C libs
#include <fcntl.h>
#include <unistd.h>

namespace {

constexpr int kBufferSize = 4 * 1024; // 4 KB

} // namespace

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

void LinuxAccessFile::Open() {
  fd_ = ::open(file_name_.c_str(), O_CREAT | O_TRUNC | O_WRONLY);
  if (fd_ == -1) {
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return;
  }
}

ssize_t LinuxAccessFile::Read() {
  char *buff = reinterpret_cast<char *>(buffer_->GetBuffer().data());
  size_t buff_len = buffer_->GetBufferLength();

  return ::read(fd_, buff, buff_len);
}

ssize_t LinuxAccessFile::Write() {
  char *buff = reinterpret_cast<char *>(buffer_->GetBuffer().data());
  size_t buff_len = buffer_->GetBufferLength();

  return ::write(fd_, buff, buff_len);
}

} // namespace kvs