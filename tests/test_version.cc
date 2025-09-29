#include <gtest/gtest.h>

#include "db/compact.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/version.h"
#include "db/version_manager.h"

// libC++
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>

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

TEST(VersionTest, CreateOnlyOneVersion) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();

  const Config *const config = db->GetConfig();

  // That number of key/value pairs is enough to create a new sst
  const int nums_elem = 10000000;

  // When db is first loaded, number of older version = 0
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 0);

  std::string key, value;
  size_t current_size = 0;
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
        immutable_memtables_in_mem = 0;
        break;
      }
    }
  }

  // // Need time for new SST is persisted to disk
  // // NOTE: It must be long enough for debug build
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Creating new SST when memtable is overlow means that new latest version
  // is created
  EXPECT_TRUE(db->GetVersionManager()->GetLatestVersion());
  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(config);
}

TEST(VersionTest, CreateMultipleVersions) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  // That number of key/value pairs will create a new sst
  const int nums_elem = 10000000;

  std::string key, value;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    db->Put(key, value, 0 /*txn_id*/);
  }

  // Force flush remaining memtable datas to SST
  db->ForceFlushMemTable();

  std::this_thread::sleep_for(std::chrono::milliseconds(10000));

  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(config);
}

TEST(VersionTest, ConcurrencyPut) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
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

  db->ForceFlushMemTable();

  std::this_thread::sleep_for(std::chrono::milliseconds(15000));

  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(config);
}

TEST(VersionTest, GetFromSST) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  // That number of key/value pairs will create a new sst
  const int nums_elem = 1000000;

  std::string key, value;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    db->Put(key, value, 0 /*txn_id*/);
  }

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();
  EXPECT_TRUE(db->GetImmutableMemTables().empty());

  // Wait until compaction finishes its job
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Now all immutable memtables are no longer in memory, it means that all GET
  // operation must go to SST to lookup

  std::optional<std::string> status;
  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    status = db->Get(key, 0 /*txn_id*/);
    EXPECT_TRUE(status);
    EXPECT_EQ(status.value(), value);
  }

  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  ClearAllSstFiles(config);
}

TEST(VersionTest, ConcurrentPutSingleGet) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  const int nums_elem_each_thread = 100000;

  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  num_threads = 24;

  const int total_elems = nums_elem_each_thread * num_threads;

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
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();

  // Sleep to wait all written data is persisted to disk
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));

  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  // Now all immutable memtables are no longer in memory, it means that all
  // GET operation must go to SST to lookup
  const Version *version = db->GetVersionManager()->GetLatestVersion();
  EXPECT_TRUE(version);

  GetStatus status;
  std::string key, value;

  for (int i = 0; i < total_elems; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    std::optional<std::string> value_found = db->Get(key, 0 /*txn_id*/);
    EXPECT_TRUE(value_found);
    EXPECT_EQ(value_found.value(), value);
  }

  ClearAllSstFiles(config);
}

TEST(VersionTest, SequentialConcurrentPutGet) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  const int nums_elem_each_thread = 100000;

  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  num_threads = 24;

  std::latch all_writes_done(num_threads);
  auto put_op = [&db, &config, nums_elem = nums_elem_each_thread,
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

  // Wait until all threads finish
  all_writes_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));

  // Now all immutable memtables are no longer in memory, it means that all
  // GET operation must go to SST to lookup
  std::latch all_reads_done(num_threads);
  auto get_op = [&db, &config, nums_elem = nums_elem_each_thread,
                 &all_reads_done](int index) {
    std::string key, value;
    std::optional<std::string> key_found;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      key_found = db->Get(key, 0 /*txn_id*/);
      if (key_found) {
      }

      EXPECT_TRUE(key_found.has_value());
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

  ClearAllSstFiles(config);
}

TEST(VersionTest, SequentialConcurrentPutDeleteGet) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  const int nums_elem_each_thread = 100000;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  num_threads = 24;

  // =========== PUT keys with values ===========
  std::latch all_put_done(num_threads);
  auto put_op = [&db, &config, nums_elem = nums_elem_each_thread,
                 &all_put_done](int index) {
    std::string key, value;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      db->Put(key, value, 0 /*txn_id*/);
    }
    all_put_done.count_down();
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(put_op, i);
  }

  // Wait until all threads finish
  all_put_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // ========== Finish PUT keys with values ===========

  // ==========Get ALL keys after PUT ==========
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
  threads.clear();
  // ========== Finish Get ALL keys after PUT ==========

  // ========== Delete all keys which were PUT ==========
  std::latch all_delete_done(num_threads);
  auto delete_op = [&db, &config, nums_elem = nums_elem_each_thread,
                    &all_delete_done](int index) {
    std::string key, value;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      db->Delete(key, 0 /*txn_id*/);
    }
    all_delete_done.count_down();
  };

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(delete_op, i);
  }
  // Wait until all threads finish
  all_delete_done.wait();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // ========== Finish Delete all keys which were PUT ==========

  // ========== Reread keys after delete ==========
  std::latch all_reads_after_delete_done(num_threads);
  auto get_after_delete_op = [&db, &config, nums_elem = nums_elem_each_thread,
                              &all_reads_after_delete_done](int index) {
    std::string key, value;
    std::optional<std::string> key_found;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      key_found = db->Get(key, 0 /*txn_id*/);

      EXPECT_TRUE(!key_found);
    }
    all_reads_after_delete_done.count_down();
  };
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(get_after_delete_op, i);
  }

  // Wait until all threads finish
  all_reads_after_delete_done.wait();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();
  // ========== Finish Reread keys after delete ==========

  // ========== Re-PUT same key ==========
  std::latch all_reput_done(num_threads);
  auto re_put_op = [&db, nums_elem = nums_elem_each_thread,
                    &all_reput_done](int index) {
    std::string key, value;
    std::optional<std::string> key_found;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      db->Put(key, value, 0 /*txn_id*/);
    }
    all_reput_done.count_down();
  };
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(re_put_op, i);
  }

  // Wait until all threads finish
  all_reput_done.wait();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // ========== Finish Re-PUT same key ==========

  // ========== Re-GET same key ==========
  std::latch all_reget_done(num_threads);
  auto re_get_op = [&db, nums_elem = nums_elem_each_thread,
                    &all_reget_done](int index) {
    std::string key, value;
    std::optional<std::string> key_found;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      key_found = db->Get(key, 0 /*txn_id*/);

      if (!key_found) {
        std::cout << "namnh2 test" << std::endl;
      }

      // EXPECT_TRUE(key_found);
      // EXPECT_EQ(key_found.value(), value);
    }
    all_reget_done.count_down();
  };

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(re_get_op, i);
  }

  // Wait until all threads finish
  all_reget_done.wait();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();
  // ========== Finish Re-GET same key ==========

  // EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));
  ClearAllSstFiles(config);
}

TEST(VersionTest, FreeObsoleteVersions) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const Config *const config = db->GetConfig();

  const int nums_elem_each_thread = 1000000;
  unsigned int num_read_threads = 10;
  unsigned int num_write_threads = 10;

  std::mutex mutex;
  std::latch all_done(num_read_threads + num_write_threads);

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

  auto read_op = [&db, &config, nums_elem = nums_elem_each_thread, &mutex,
                  &all_done](int index) {
    std::string key, value;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      std::optional<std::string> value_found = db->Get(key, 0 /*txn_id*/);
      if (value_found) {
        EXPECT_EQ(value_found.value(), value);
      }
    }
    all_done.count_down();
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_write_threads; i++) {
    threads.emplace_back(put_op, i);
  }

  for (int i = 0; i < num_read_threads; i++) {
    threads.emplace_back(read_op, i);
  }

  // Wait until all threads finish
  all_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }

  // Force clearing all immutable memtables
  db->ForceFlushMemTable();

  // Sleep to wait all older versions is not referenced anymore
  std::this_thread::sleep_for(std::chrono::milliseconds(15000));

  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(config, db.get()));
  // All older versions that aren't refered to anymore should be cleared
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 0);

  ClearAllSstFiles(config);
}

} // namespace db

} // namespace kvs