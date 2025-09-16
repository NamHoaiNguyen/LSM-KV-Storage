#include "db/version_edit.h"

#include "sstable/table_builder.h"

namespace kvs {

namespace db {
void VersionEdit::AddNewFiles(SSTId sst_id, int level, uint64_t file_size,
                              std::string_view smallest_key,
                              std::string_view laragest_key,
                              std::shared_ptr<sstable::TableBuilder> table) {
  auto sst_metadata = std::make_shared<SSTMetadata>();
  sst_metadata->sst_id = sst_id;
  sst_metadata->level = level;
  sst_metadata->file_size = file_size;
  sst_metadata->smallest_key = std::string(smallest_key);
  sst_metadata->largest_key = std::string(laragest_key);
  sst_metadata->table_ = std::move(table);

  new_files_.push_back(std::move(sst_metadata));
}

void VersionEdit::RemoveFiles(int level, SSTId sst_id) {
  deleted_files_.insert({sst_id, level});
}

const std::set<std::pair<SSTId, int>> &VersionEdit::GetImmutableDeletedFiles() {
  return deleted_files_;
}

const std::vector<std::shared_ptr<SSTMetadata>> &
VersionEdit::GetImmutableNewFiles() {
  return new_files_;
}

} // namespace db

} // namespace kvs