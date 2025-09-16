#include "db/version_edit.h"

#include "sstable/table_reader.h"

namespace kvs {

namespace db {

void VersionEdit::AddNewFiles(
    SSTId table_id, int level, uint64_t file_size,
    std::string_view smallest_key, std::string_view largest_key,
    std::string &&filename,
    std::shared_ptr<sstable::TableBuilder> table_builder) {
  auto sst_metadata = std::make_shared<SSTMetadata>();
  sst_metadata->table_id = table_id;
  sst_metadata->level = level;
  sst_metadata->file_size = file_size;
  sst_metadata->smallest_key = std::string(smallest_key);
  sst_metadata->largest_key = std::string(largest_key);
  sst_metadata->table_builder = table_builder;
  sst_metadata->table_ =
      std::make_shared<sstable::TableReader>(std::move(filename), file_size);
  sst_metadata->table_->Open();

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