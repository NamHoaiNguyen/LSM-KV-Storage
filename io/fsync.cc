#include "fsync.h"

namespace kvs {

int FSync::FsyncFlush(Fd fd) {
  int err = fsync(fd);
  return err
}

void FSync::FsyncFlushBatch(const std::vector<Fd> fds) {
  for (const auto& fd : fds) {
    //  TODO(namnh, handle error)
    fsync(fd);
  }
}

} // namespace kvs