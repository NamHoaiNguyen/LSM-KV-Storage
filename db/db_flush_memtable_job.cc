#include "db/db_impl.h"

#include "io/buffer.h"
#include "io/linux_file.h"

// libC++
#include <mutex>

namespace kvs {

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  std::string path = "~/lsm-kv-storage/data/000001.sst";
  auto linux_object_file = std::make_unique<LinuxAccessFile>(std::move(path));

  // Create new sstable level 0
  if (!linux_object_file->Open()) {
    return;
  }

  {
    // Need to acquire lock when moving data from immutable into buffer
    std::scoped_lock rwlock_immutable_tables(immutable_memtables_mutex_);
  }
}

} // namespace kvs