#include <gtest/gtest.h>

#include "db/config.h"
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/skiplist.h"
#include "db/version_manager.h"

// libC++
#include <filesystem>
#include <memory>

// posix API
#include <sys/resource.h>

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

  return (num_sst_files == num_sst_files_info + 1 /*include manifest file*/)
             ? true
             : false;
}

void ClearAllSstFiles(const DBImpl *db) {
  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

void PutOp(db::DBImpl *db, int nums_elem, int index, std::latch &done) {
  std::string key, value;

  for (size_t i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(nums_elem * index + i);
    value = "value" + std::to_string(nums_elem * index + i);
    db->Put(key, value, 0 /*txn_id*/);
  }
  done.count_down();
}

// void GetOp(db::DBImpl *db, int nums_elem, int index, std::latch &done) {
//   std::string key, value;
//   GetStatus status;

//   for (size_t i = 0; i < nums_elem; i++) {
//     key = "key" + std::to_string(nums_elem * index + i);
//     value = "value" + std::to_string(nums_elem * index + i);
//     status = db->Get(key, 0 /*txn_id*/);

//     EXPECT_TRUE(status.type == ValueType::PUT ||
//                 status.type == ValueType::DELETED);
//     EXPECT_EQ(status.value.value(), value);
//   }
//   done.count_down();
// }

void GetWithRetryOp(db::DBImpl *db, int nums_elem, int index,
                    std::latch &done) {
  std::string key, value;
  GetStatus status;
  std::vector<std::pair<std::string, std::string>> miss_keys;

  for (size_t i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(nums_elem * index + i);
    value = "value" + std::to_string(nums_elem * index + i);
    status = db->Get(key, 0 /*txn_id*/);

    if (status.type == ValueType::kTooManyOpenFiles) {
      int retry = 0;
      while (retry < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        status = db->Get(key, 0 /*txn_id*/);
        if (status.type == ValueType::PUT ||
            status.type == ValueType::DELETED) {
          break;
        }

        retry++;
      }

      if (status.type == ValueType::kTooManyOpenFiles) {
        miss_keys.push_back({key, value});
        continue;
      }
    } else if (status.type == ValueType::NOT_FOUND) {
      std::cout << "namnh key missed " << key << std::endl;
      miss_keys.push_back({key, value});
      continue;
    }

    EXPECT_EQ(status.type, ValueType::PUT);
    EXPECT_EQ(status.value.value(), value);
  }

  // // Wait a little bit
  // std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  std::cout << "namnh GetWithRetryOp after sleep 5s" << std::endl;

  for (int i = 0; i < miss_keys.size(); i++) {
    status = db->Get(key, 0 /*txn_id*/);
    EXPECT_TRUE(status.type == ValueType::PUT);
    EXPECT_EQ(status.value.value(), value);
  }

  done.count_down();
}

TEST(DBTest, LRUTableReaderCache) {
  // Guarantee that maximum number of file descriptors doesn't exceed limit
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  const int nums_elem_each_thread = 1000000;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  std::vector<std::thread> threads;

  std::latch all_writes_done(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_writes_done));
  }
  // Wait until all threads finish
  all_writes_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();
  // Wait a little bit time
  // std::this_thread::sleep_for(std::chrono::milliseconds(10000));

  // Set new limits
  // With that value of soft limit, it is ensured that table reader cache MUST
  // work normally to guarantee that this threshold is not reached.
  struct rlimit limit;
  limit.rlim_cur = 1024;
  limit.rlim_max = 4096;

  EXPECT_NE(setrlimit(RLIMIT_NOFILE, &limit), -1);

  std::latch all_reads_done(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_reads_done));
  }

  // Wait until all threads finish
  all_reads_done.wait();

  std::cout << "LRUTableReaderCache all_reads_done finished " << std::endl;

  // Wait a little bit for compaction. Otherwise, test is crashed
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // // Wait a little bit for compaction. Otherwise, test is crashed
  // std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  // Number of SST files in directory should be equal to number of SST files in
  // version after reloading
  // EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(db.get());
}

TEST(DBTest, RecoverDB) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  const int nums_elem_each_thread = 500000;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  std::vector<std::thread> threads;

  std::latch all_writes_done(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_writes_done));
  }
  // Wait until all threads finish
  all_writes_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();
  // Wait a little bit time
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  std::latch all_reads_done(1);
  for (int i = 0; i < 1; i++) {
    threads.emplace_back(GetWithRetryOp, db.get(),
                         nums_elem_each_thread * num_threads, i,
                         std::ref(all_reads_done));
  }

  // Wait until all threads finish
  all_reads_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Wait a little bit for compaction. Otherwise, test is crashed
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  // Restarting a new db instance
  db.reset();
  // Check recovery
  db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  std::latch all_reget_done(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_reget_done));
  }

  // Wait until all threads finish
  all_reget_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }

  // Number of SST files in directory should be equal to number of SST files in
  // version after reloading
  // EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(db.get());
}

TEST(DBTest, MultipleDB) {
  auto db1 = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db1->LoadDB("test1");

  const int nums_elem_each_thread = 100000;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  const int total_elems = nums_elem_each_thread * num_threads;

  std::latch all_writes_done_1(num_threads);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOp, db1.get(), nums_elem_each_thread, i,
                         std::ref(all_writes_done_1));
  }
  all_writes_done_1.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db1->ForceFlushMemTable();
  // Wait a little bit time
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::latch all_reads_done_1(num_threads);
  auto get_op = [&db1, nums_elem = nums_elem_each_thread,
                 &all_reads_done_1](int index) {
    std::string key, value;
    GetStatus status;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      status = db1->Get(key, 0 /*txn_id*/);

      EXPECT_EQ(status.type, ValueType::PUT);
      EXPECT_EQ(status.value.value(), value);
    }
    all_reads_done_1.count_down();
  };

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOp, db1.get(), nums_elem_each_thread, i,
                         std::ref(all_reads_done_1));
  }

  // Wait until all threads finish
  all_reads_done_1.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Creatomg a new db instance
  // Check recovery
  auto db2 = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db2->LoadDB("test2");

  std::latch all_writes_done_2(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOp, db2.get(), nums_elem_each_thread, i,
                         std::ref(all_writes_done_2));
  }
  all_writes_done_2.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db1->ForceFlushMemTable();
  // Wait a little bit time
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::latch all_reads_done_2(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOp, db2.get(), nums_elem_each_thread, i,
                         std::ref(all_reads_done_2));
  }

  // Wait until all threads finish
  all_reads_done_2.wait();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Number of SST files in directory should be equal to number of SST files in
  // version after reloading
  // EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(db1.get());
  ClearAllSstFiles(db2.get());
}

} // namespace db

} // namespace kvs