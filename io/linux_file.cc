#include "io/linux_file.h"

// C libs
#include <unistd.h>

namespace kvs {

LinuxAccessFile::LinuxAccessFile(std::string&& file_name)
    : file_name_(file_name) {}

LinuxAccessFile::~LinuxAccessFile() {
  Close();
}

void LinuxAccessFile::Append() {}

void LinuxAccessFile::Close() {
  if (fd_ < = 0) {
    return;
  }

  if (::close(fd_) < 0) {

  }
}

void LinuxAccessFile::Flush() {
  if (::fsync(fd_) < 0) {
    
  }
}

void LinuxAccessFile::Open() {
  fd_ = ::open(file_name_.c_str(), O_CREAT | O_TRUNC | O_WRONLY);
}

} // namespace kvs