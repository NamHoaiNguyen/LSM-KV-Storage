#ifndef IO_FSYNC_H
#define IO_FSYNC_H

#include "common/macros.h"

namespace kvs {

class FSync {
  public:
    FSync() = default;

  private:
    int FsyncFlush(Fd fd);

    void FsyncFlushBatch(const std::vector<Fd> fds);
};

} // namespace kvs

#endif // IO_FSYNC_H