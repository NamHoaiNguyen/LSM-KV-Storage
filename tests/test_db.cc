#include <gtest/gtest.h>

#include "db/config.h"
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/skiplist.h"
#include "db/version_manager.h"

// libC++
#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

bool CompareVersionFilesWithDirectoryFiles(const Config *config, DBImpl *db) {
  int num_sst_files = 0;
  int num_sst_files_info = 0;

  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      num_sst_files++;
    }
  }

  for (const auto &sst_file_info :
       db->GetVersionManager()->GetLatestVersion()->GetImmutableSSTMetadata()) {
    num_sst_files_info += sst_file_info.size();
  }

  return (num_sst_files == num_sst_files_info + 1 /*include manifest file*/)
             ? true
             : false;
}

void ClearAllSstFiles(const Config *config) {
  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(config->GetSavedDataPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

TEST(DBTest, RecoverDB) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();

  const int nums_elem_each_thread = 1000000;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  num_threads = 24;
  const int total_elems = nums_elem_each_thread * num_threads;

  std::latch all_writes_done(num_threads);

  auto put_op = [&db, nums_elem = nums_elem_each_thread,
                 &all_writes_done](int index) {
    std::string key, value;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      db->Put(key, value, 0 /*txn_id*/);
    }
    all_writes_done.count_down();
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(put_op, i);
  }

  all_writes_done.wait();

  // // Force clearing all immutable memtables
  db->ForceFlushMemTable();

  // Wait a little bit time
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  // Restarting a new db instance
  db.reset();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Check recovery
  db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  std::latch all_reads_done(num_threads);
  auto get_op = [&db, nums_elem = nums_elem_each_thread,
                 &all_reads_done](int index) {
    std::string key, value;
    std::optional<std::string> key_found;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      key_found = db->Get(key, 0 /*txn_id*/);
      EXPECT_TRUE(key_found);
      EXPECT_EQ(key_found.value(), value);
    }
    all_reads_done.count_down();
  };

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(get_op, i);
  }

  // Wait until all threads finish
  all_reads_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }

  // Number of SST files in directory should be equal to number of SST files in
  // version after reloading
  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(config);
}

} // namespace db

} // namespace kvs