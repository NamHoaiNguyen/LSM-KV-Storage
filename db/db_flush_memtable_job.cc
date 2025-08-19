#include "db/db_impl.h"

#include "io/buffer.h"
#include "io/linux_file.h"
#include "sstable/table.h"

// libC++
#include <mutex>

namespace kvs {

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  std::string path = "~/lsm-kv-storage/data/000001.sst";

  auto sstable = std::make_unique<Table>();
  sstable->Encode(immutable_memtables_.at(immutable_memtable_index).get());

  {
    // Need to acquire lock when moving data from immutable into buffer
    std::scoped_lock rwlock_immutable_tables(immutable_memtables_mutex_);
  }
}

} // namespace kvs