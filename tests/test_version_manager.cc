#include <gtest/gtest.h>

#include "db/compact.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/version.h"
#include "db/version_manager.h"

// libC++
#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

TEST(VersionManagerTest, BasicOperation) {
  auto db = std::make_unique<db::DBImpl>();
  db->LoadDB();

  const Config *const config = db->GetConfig();
  // That number of key/value pairs is enough to create a new sst
  const int nums_elem = 300000;

  // When db is first loaded, number of older version = 0
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 0);

  std::string key, value;
  size_t current_size = 0;
  // Include latest_version
  int number_version = 1;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    db->Put(key, value, 0 /*txn_id*/);

    current_size += key.size() + value.size();
    if (current_size >= config->GetPerMemTableSizeLimit()) {
      // Only create 1 SST
      number_version++;
      current_size = 0;
      break;
    }
  }

  // Need time for new SST is persisted to disk
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  // Creating new SST when memtable is overlow means that new latest version is
  // created
  EXPECT_TRUE(db->GetVersionManager()->GetLatestVersion());
  // Just latest version is created
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 1);

  int num_sst_files = 0;
  int num_sst_files_info = 0;

  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      num_sst_files++;
    }
  }

  // Total number of versions = 2.
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size() + 1
            /*latest_version*/,
            number_version);
  EXPECT_EQ(number_version, 2);

  for (const auto &sst_file_info :
       db->GetVersionManager()->GetLatestVersion()->GetImmutableSSTInfo()) {
    num_sst_files_info += sst_file_info.size();
  }

  EXPECT_EQ(num_sst_files, num_sst_files_info);

  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

TEST(VersionManagerTest, LatestVersion) {
  auto db = std::make_unique<db::DBImpl>();
  db->LoadDB();
  const Config *const config = db->GetConfig();
  // That number of key/value pairs will create a new sst
  const int nums_elem = 10000000;

  std::string key, value;
  size_t current_size = 0;
  int number_version = 1;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    current_size += key.size() + value.size();
    if (current_size >= config->GetPerMemTableSizeLimit()) {
      number_version++;
      current_size = 0;
    }

    db->Put(key, value, 0 /*txn_id*/);
  }

  // std::this_thread::sleep_for(std::chrono::milliseconds(7000));

  int num_sst_files = 0;
  int num_sst_files_info = 0;

  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      num_sst_files++;
    }
  }

  EXPECT_EQ(db->GetVersionManager()->GetVersions().size() +
                1 /*latest_version*/,
            number_version);
  for (const auto &sst_file_info :
       db->GetVersionManager()->GetLatestVersion()->GetImmutableSSTInfo()) {
    num_sst_files_info += sst_file_info.size();
  }

  EXPECT_EQ(num_sst_files, num_sst_files_info);

  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

} // namespace db

} // namespace kvs