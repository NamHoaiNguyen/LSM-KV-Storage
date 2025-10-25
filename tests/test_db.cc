#include <gtest/gtest.h>

#include "db/config.h"
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/skiplist.h"
#include "db/version_manager.h"

// libC++
#include <chrono>
#include <filesystem>
#include <memory>

// posix API
#include <sys/resource.h>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

void ClearAllSstFiles(const DBImpl *db) {
  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

std::string generateRandomString(size_t length) {
  const std::string characters =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::string randomString;

  for (size_t i = 0; i < length; ++i) {
    randomString += characters[rand() % characters.length()];
  }

  return randomString;
}

void PutOp(db::DBImpl *db, int nums_elem, int index, std::latch &done) {
  std::string key, value;

  std::srand(static_cast<unsigned int>(std::time(0)));
  for (size_t i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(nums_elem * index + i);
    value = "value" + std::to_string(nums_elem * index + i);
    db->Put(key, value, 0 /*txn_id*/);
  }
  done.count_down();
}

void PutOpV2(db::DBImpl *db, int nums_elem, int index,
             const std::vector<std::pair<std::string, std::string>> &key_value,
             std::latch &done) {
  for (size_t i = 0; i < nums_elem; i++) {
    db->Put(key_value[nums_elem * index + i].first,
            key_value[nums_elem * index + i].second, 0 /*txn_id*/);
  }
  done.count_down();
}

void DeleteOp(db::DBImpl *db, int nums_elem, int index, std::latch &done) {
  std::string key, value;

  for (size_t i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(nums_elem * index + i);
    db->Delete(key, 0 /*txn_id*/);
  }
  done.count_down();
}

void GetWithRetryOp(db::DBImpl *db, int nums_elem, int index,
                    std::latch &done) {
  std::string key, value;
  GetStatus status;
  std::vector<std::pair<std::string, std::string>> miss_keys;

  for (size_t i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(nums_elem * index + i);
    value = "value" + std::to_string(nums_elem * index + i);
    status = db->Get(key, 0 /*txn_id*/);

    EXPECT_TRUE(status.type == ValueType::PUT ||
                status.type == ValueType::DELETED ||
                status.type == ValueType::kTooManyOpenFiles);

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
    } else if (status.type == ValueType::PUT) {
      EXPECT_EQ(status.value.value(), value);
    } else {
      EXPECT_FALSE(status.value);
    }
  }

  for (int i = 0; i < miss_keys.size(); i++) {
    status = db->Get(key, 0 /*txn_id*/);
    EXPECT_TRUE(status.type == ValueType::PUT);
    EXPECT_EQ(status.value.value(), value);
  }

  done.count_down();
}

void GetWithRetryOpV2(
    db::DBImpl *db, int nums_elem, int index,
    const std::vector<std::pair<std::string, std::string>> &key_value,
    std::latch &done) {
  std::string key, value;
  GetStatus status;
  std::vector<std::pair<std::string, std::string>> miss_keys;

  for (size_t i = 0; i < nums_elem; i++) {
    std::string_view key = key_value[nums_elem * index + i].first;
    std::string_view value = key_value[nums_elem * index + i].second;

    status = db->Get(key, 0 /*txn_id*/);

    EXPECT_TRUE(status.type == ValueType::PUT ||
                status.type == ValueType::DELETED ||
                status.type == ValueType::kTooManyOpenFiles);

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
        miss_keys.emplace_back(std::make_pair(key, value));
        continue;
      }
    } else if (status.type == ValueType::PUT) {
      EXPECT_EQ(status.value.value(), value);
    } else {
      EXPECT_FALSE(status.value);
    }
  }

  for (int i = 0; i < miss_keys.size(); i++) {
    status = db->Get(key, 0 /*txn_id*/);
    EXPECT_TRUE(status.type == ValueType::PUT);
    EXPECT_EQ(status.value.value(), value);
  }

  done.count_down();
}

TEST(DBTest, ConcurrencyPutAndGetLargeKeyValue) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");
  const Config *const config = db->GetConfig();

  const int nums_elem_each_thread = 100000;

  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }

  std::vector<std::thread> threads;
  std::vector<std::pair<std::string, std::string>> key_value;

  auto start_gen_data = std::chrono::high_resolution_clock::now();
  std::mutex mutex;
  std::latch all_gen_data_done(num_threads);

  auto gen_data = [&key_value, &mutex, &all_gen_data_done,
                   nums_elem_each_thread]() {
    for (int i = 0; i < nums_elem_each_thread; i++) {
      std::string key = generateRandomString(100);
      std::string value = generateRandomString(100);
      {
        std::scoped_lock rwlock(mutex);
        key_value.push_back({std::move(key), std::move(value)});
      }
    }
    all_gen_data_done.count_down();
  };
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(gen_data);
  }
  all_gen_data_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  auto end_gen_data = std::chrono::high_resolution_clock::now();

  std::latch all_done(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOpV2, db.get(), nums_elem_each_thread, i,
                         std::cref(key_value), std::ref(all_done));
  }

  // Wait until all threads finish
  all_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  std::latch all_reads_done(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOpV2, db.get(), nums_elem_each_thread, i,
                         std::cref(key_value), std::ref(all_reads_done));
  }

  // Wait until all threads finish
  all_reads_done.wait();
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  auto end = std::chrono::high_resolution_clock::now();
  // Calculate the duration
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - end_gen_data);

  std::cout << "Execution time: " << duration.count() << " ms" << std::endl;

  // Wait until compaction finishes it job
  std::this_thread::sleep_for(std::chrono::milliseconds(15000));

  ClearAllSstFiles(db.get());
}

TEST(DBTest, SequentialConcurrentPutDeleteGet) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  ClearAllSstFiles(db.get());

  const int nums_elem_each_thread = 1000000;
  unsigned int num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) {
    // std::thread::hardware_concurrency() might return 0 if sys info not
    // available
    num_threads = 10;
  }
  num_threads = 24;

  // =========== PUT keys with values ===========
  std::latch all_put_done(num_threads);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_put_done));
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
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_reads_done));
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
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(DeleteOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_delete_done));
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
  auto get_after_delete_op = [&db, nums_elem = nums_elem_each_thread,
                              &all_reads_after_delete_done](int index) {
    std::string key, value;
    GetStatus status;

    for (size_t i = 0; i < nums_elem; i++) {
      key = "key" + std::to_string(nums_elem * index + i);
      value = "value" + std::to_string(nums_elem * index + i);
      status = db->Get(key, 0 /*txn_id*/);

      EXPECT_TRUE(status.type == ValueType::DELETED ||
                  status.type == ValueType::NOT_FOUND);
      EXPECT_FALSE(status.value);
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
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(PutOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_reput_done));
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
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(GetWithRetryOp, db.get(), nums_elem_each_thread, i,
                         std::ref(all_reget_done));
  }

  // Wait until all threads finish
  all_reget_done.wait();

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();
  // ========== Finish Re-GET same key ==========
  // Wait a litte bit. Compaction may still be running
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  ClearAllSstFiles(db.get());
}

TEST(DBTest, LRUTableReaderCache) {
  // Guarantee that maximum number of file descriptors doesn't exceed limit
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  ClearAllSstFiles(db.get());

  const int nums_elem_each_thread = 3000000;
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

  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // // Wait a little bit for compaction.
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  // Number of SST files in directory should be equal to number of SST files in
  // version after reloading
  ClearAllSstFiles(db.get());
}

TEST(DBTest, RecoverDB) {
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
  ClearAllSstFiles(db1.get());
  ClearAllSstFiles(db2.get());
}

} // namespace db

} // namespace kvs