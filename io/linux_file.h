#ifndef IO_LINUX_FILE_OBJECT_H
#define IO_LINUX_FILE_OBJECT_H

#include "common/macros.h"
#include "io/base_file.h"

#include <string>

namespace kvs {
  class LinuxAccessFile : public AccessFile {
    public:
    LinuxAccessFile(std::string&& file_Name);

    ~LinuxAccessFile();
  
    void Append() override;

    void Close() override;

    void Flush() override;

    void Open() override;

    private:
      std::string file_name_;

      Fd fd_; // file descriptor
  };
} // namespace kvs

#endif // IO_LINUX_FILE_OBJECT_H