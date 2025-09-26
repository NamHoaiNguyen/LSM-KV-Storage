#include "db/version_edit.h"

namespace kvs {

namespace db {

VersionEdit::VersionEdit(int num_levels) : new_files_(num_levels) {}

SSTMetadata::SSTMetadata(SSTId table_id_, int level_, uint64_t file_size_,
                         std::string_view smallest_key_,
                         std::string_view largest_key_, std::string &&filename_)
    : table_id(table_id_), level(level_), file_size(file_size_),
      smallest_key(std::string(smallest_key_)),
      largest_key(std::string(largest_key_)), filename(std::move(filename_)) {}

void VersionEdit::AddNewFiles(SSTId table_id, int level, uint64_t file_size,
                              std::string_view smallest_key,
                              std::string_view largest_key,
                              std::string &&filename) {
  auto sst_metadata =
      std::make_shared<SSTMetadata>(table_id, level, file_size, smallest_key,
                                    largest_key, std::move(filename));
  new_files_[sst_metadata->level].push_back(std::move(sst_metadata));
}

void VersionEdit::RemoveFiles(int level, SSTId sst_id) {
  deleted_files_.insert({sst_id, level});
}

const std::set<std::pair<SSTId, int>> &VersionEdit::GetImmutableDeletedFiles() {
  return deleted_files_;
}

const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
VersionEdit::GetImmutableNewFiles() {
  return new_files_;
}

} // namespace db

} // namespace kvs