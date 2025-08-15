#ifndef IO_FILE_OBJECT_H
#define IO_FILE_OBJECT_H

namespace kvs {
  class AccessFile {
    virtual ~AccessFile() {}

    virtual void Append() = 0;

    virtual void Close() = 0;

    virtual void Flush() = 0;

    virtual void Open() = 0;
  };
} // namespace kvs

#endif // IO_FILE_OBJECT_H