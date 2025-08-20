#include "db/db_impl.h"

#include "db/memtable_iterator.h"
#include "io/buffer.h"
#include "io/linux_file.h"
#include "sstable/table_builder.h"

// libC++
#include <mutex>

namespace kvs {

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  std::string path = "~/lsm-kv-storage/data/000001.sst";

  auto builder = std::make_unique<TableBuilder>();
  // TODO(namnh) : unique_ptr or shared_ptr?
  auto iterator = std::make_unique<MemTableIterator>();
  for (iterator->SeekToFirst(); iterator != IsValid(); iterator->Next()) {
    builder->Add(iterator->GetKey(), iterator->GetValue());
  }

  {
    // Need to acquire lock when moving data from immutable into buffer
    std::scoped_lock rwlock_immutable_tables(immutable_memtables_mutex_);
  }
}

} // namespace kvs