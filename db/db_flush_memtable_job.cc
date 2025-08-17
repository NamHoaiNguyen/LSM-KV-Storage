#include "db/db_impl.h"

namespace kvs {

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  // No need to acquire lock because write operations aren't
  // allowed in immutable memtables.
}

} // namespace kvs