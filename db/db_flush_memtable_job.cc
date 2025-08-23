#include "db/db_impl.h"

#include "db/memtable_iterator.h"
#include "io/buffer.h"
#include "io/linux_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/table.h"

// libC++
#include <mutex>

namespace kvs {

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  std::string path = "~/lsm-kv-storage/data/000001.sst";

  auto sstable = std::make_unique<Table>(std::move(path));
  // TODO(namnh) : unique_ptr or shared_ptr?
  const BaseMemTable *immutable_memtable =
      immutable_memtables_.at(immutable_memtable_index).get();
  auto iterator = std::make_unique<MemTableIterator>(immutable_memtable);

  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    sstable->AddEntry(iterator->GetKey(), iterator->GetValue(),
                      iterator->GetTransactionId());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  sstable->Finish();

  {
    // Need to acquire lock when moving data from immutable into buffer
    std::scoped_lock rwlock_immutable_tables(immutable_memtables_mutex_);
  }
}

} // namespace kvs