#include <gtest/gtest.h>

#include "db/compact.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/version.h"
#include "db/version_manager.h"

// libC++
#include <filesystem>
#include <memory>
#include <thread>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

TEST(VersionManagerTest, CreateOnlyOneVersion) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();

  const Config *const config = db->GetConfig();
  // That number of key/value pairs is enough to create a new sst
  const int nums_elem = 10000000;

  // When db is first loaded, number of older version = 0
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 0);

  std::string key, value;
  size_t current_size = 0;
  // Include latest_version
  int number_version = 1;
  int immutable_memtables_in_mem = 0;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    db->Put(key, value, 0 /*txn_id*/);

    current_size += key.size() + value.size();
    if (current_size >= config->GetPerMemTableSizeLimit()) {
      current_size = 0;
      immutable_memtables_in_mem++;
      if (immutable_memtables_in_mem >= config->GetMaxImmuMemTablesInMem()) {
        // Stop immediately if flushing is triggered
        number_version++;
        immutable_memtables_in_mem = 0;
        break;
      }
    }
  }

  // Need time for new SST is persisted to disk
  // NOTE: It must be long enough for debug build
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

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

TEST(VersionManagerTest, CreateMultipleVersions) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();
  // That number of key/value pairs will create a new sst
  const int nums_elem = 10000000;

  std::string key, value;
  size_t current_size = 0;
  int number_version = 1;
  int immutable_memtables_in_mem = 0;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    current_size += key.size() + value.size();
    if (current_size >= config->GetPerMemTableSizeLimit()) {
      current_size = 0;
      immutable_memtables_in_mem++;
      if (immutable_memtables_in_mem >= config->GetMaxImmuMemTablesInMem()) {
        number_version++;
        immutable_memtables_in_mem = 0;
      }
    }

    db->Put(key, value, 0 /*txn_id*/);
  }

  db->ForceFlushMemTable();
  number_version++;

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

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

TEST(VersionManagerTest, Concurrency) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();
  const int nums_elem_each_thread = 1000000;
  int number_version = 1;
  size_t current_size = 0;
  int immutable_memtables_in_mem = 0;

  std::mutex mutex;

  // unsigned int num_threads = 10;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }

  std::latch all_done(num_threads);

  auto put_op = [&db, &config, nums_elem = nums_elem_each_thread,
                 &number_version, &current_size, &immutable_memtables_in_mem,
                 &mutex, &all_done](int index) {
    std::string key, value;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);

      std::scoped_lock lock(mutex);
      current_size += key.size() + value.size();
      if (current_size >= config->GetPerMemTableSizeLimit()) {
        current_size = 0;
        immutable_memtables_in_mem++;
        if (immutable_memtables_in_mem >= config->GetMaxImmuMemTablesInMem()) {
          number_version++;
          immutable_memtables_in_mem = 0;
        }
      }

      db->Put(key, value, 0 /*txn_id*/);
    }
    all_done.count_down();
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(put_op, i);
  }

  // Wait until all threads finish
  all_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }

  db->ForceFlushMemTable();
  number_version++;

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