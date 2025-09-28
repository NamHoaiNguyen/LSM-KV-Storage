#include <gtest/gtest.h>

#include "db/config.h"
#include "db/db_impl.h"
#include "db/version_edit.h"
#include "io/linux_file.h"

// libC++
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

void ClearAllSstFiles(const Config *config) {
  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

TEST(VersionTest, BasicEncodeManifest) {
  auto db = std::make_unique<DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const db::Config *const config = db->GetConfig();
  auto version_edit = std::make_unique<VersionEdit>(config->GetSSTNumLvels());

  const int num_added_files = 3;
  for (int i = 0; i < num_added_files; i++) {
    SSTId table_id = i + 1;
    int level = (i + 1) % 2;
    uint64_t file_size = (i + 1) * 100000;
    std::string smallest_key = "key" + std::to_string(i + 1);
    std::string largest_key = "key" + std::to_string((i + 1) * 100000);
    std::string filename = std::to_string(table_id) + ".sst";

    version_edit->AddNewFiles(table_id, level, file_size, smallest_key,
                              largest_key, std::move(filename));
  }

  const int num_deleted_files = 2;
  for (int i = num_added_files + 1; i <= num_added_files + num_deleted_files;
       i++) {
    SSTId table_id = i;
    int level = i % 2;

    version_edit->RemoveFiles(table_id, level);
  }

  version_edit->SetNextTableId(6);
  db->AddChangesToManifest(version_edit.get());

  std::string expected =
      R"({"next_table_id":6,"new_files":[)"
      R"({"id":2,"level":0,"size":200000,"smallest_key":"key2","largest_key":"key200000"},)"
      R"({"id":1,"level":1,"size":100000,"smallest_key":"key1","largest_key":"key100000"},)"
      R"({"id":3,"level":1,"size":300000,"smallest_key":"key3","largest_key":"key300000"}],)"
      R"("delete_files":[)"
      R"({"id":4,"level":0},)"
      R"({"id":5,"level":1}]})";

  auto read_manifest_object = std::make_unique<io::LinuxReadOnlyFile>(
      config->GetSavedDataPath() + "MANIFEST");
  read_manifest_object->Open();

  std::vector<Byte> read_buffer(expected.size(), 0);
  uint64_t offset = 0;
  std::string encoded_result;

  ssize_t bytes_read = read_manifest_object->RandomRead(read_buffer, offset);
  encoded_result.append(reinterpret_cast<const char *>(read_buffer.data()),
                        read_buffer.size());

  EXPECT_EQ(encoded_result, expected);
  ClearAllSstFiles(config);
}

} // namespace db

} // namespace kvs