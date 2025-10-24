#include "sstable/lru_table_item.h"

#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

#include <iostream>

namespace kvs {

namespace sstable {

LRUTableItem::LRUTableItem(SSTId table_id,
                           std::unique_ptr<TableReader> table_reader,
                           const TableReaderCache *const cache)
    : ref_count_(0), table_id_(table_id),
      table_reader_(std::move(table_reader)), cache_(cache) {}

uint64_t LRUTableItem::GetRefCount() const { return ref_count_.load(); }

void LRUTableItem::IncRef() const { ref_count_.fetch_add(1); }

void LRUTableItem::Unref() const {
  if (ref_count_.fetch_sub(1) == 1) {
    cache_->AddVictim(table_id_);
  }
}

const TableReader *LRUTableItem::GetTableReader() const {
  return table_reader_.get();
}

} // namespace sstable

} // namespace kvs