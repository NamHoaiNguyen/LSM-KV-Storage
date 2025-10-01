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

bool CompareVersionFilesWithDirectoryFiles(const DBImpl *db) {
  int num_sst_files = 0;
  int num_sst_files_info = 0;

  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      num_sst_files++;
    }
  }

  for (const auto &sst_file_info :
       db->GetVersionManager()->GetLatestVersion()->GetImmutableSSTMetadata()) {
    num_sst_files_info += sst_file_info.size();
  }

  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }

  return (num_sst_files == num_sst_files_info) ? true : false;
}

void ClearAllSstFiles(const DBImpl *db) {
  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

TEST(CompactTest, CompactLv0Lvl1) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  // db->LoadDB();
  db->LoadDB("test");
  const Config *const config = db->GetConfig();
  const int nums_elem_each_thread = 1000000;

  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }

  std::mutex mutex;
  std::latch all_done(num_threads);

  auto put_op = [&db, &config, nums_elem = nums_elem_each_thread, &mutex,
                 &all_done](int index) {
    std::string key, value;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
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

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();

  // Compaction should be triggered
  EXPECT_TRUE(db->GetVersionManager()->NeedSSTCompaction() == true);

  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(db.get()));
  // All older versions that aren't refered to anymore should be cleared
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 0);

  ClearAllSstFiles(db.get());
}

} // namespace db

} // namespace kvs