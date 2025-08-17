#include "db/db_impl.h"

namespace kvs {

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  // Don't need to acquire lock because write operations aren't
  // allowed in immutable memtables.
}

} // namespace kvs