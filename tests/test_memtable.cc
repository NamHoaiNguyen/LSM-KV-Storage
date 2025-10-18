#include <gtest/gtest.h>

#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/skiplist.h"
#include "db/skiplist_iterator.h"
#include "db/status.h"

#include <memory>

namespace kvs {

TEST(MemTableTest, BasicOperations) {
  auto memtable = std::make_unique<db::MemTable>(1 /*memtable_version*/);

  memtable->Put("k1", "v1", 0);
  EXPECT_TRUE(memtable->Get("k1", 0).type == db::ValueType::PUT);
  EXPECT_EQ(memtable->Get("k1", 0).value, "v1");

  // update
  memtable->Put("k1", "v2", 0);
  EXPECT_TRUE(memtable->Get("k1", 0).type == db::ValueType::PUT);
  EXPECT_EQ(memtable->Get("k1", 0).value, "v2");

  // delete
  memtable->Delete("k1", 0);
  EXPECT_TRUE(memtable->Get("k1", 0).type == db::ValueType::DELETED);
  EXPECT_TRUE(memtable->Get("k1", 0).value == std::nullopt);
}

TEST(MemTableTest, LargeScalePutAndGet) {
  auto memtable = std::make_unique<db::MemTable>(1 /*memtable_version*/);

  const int num_keys = 100000;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    memtable->Put(key, value, 0);
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    EXPECT_TRUE(memtable->Get(key, 0).type == db::ValueType::PUT);
    EXPECT_EQ(memtable->Get(key, 0).value, value);
  }

  // Update
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i + num_keys);
    memtable->Put(key, value, 0);
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i + num_keys);

    EXPECT_TRUE(memtable->Get(key, 0).type == db::ValueType::PUT);
    EXPECT_EQ(memtable->Get(key, 0).value, value);
  }
}

TEST(MemTableTest, LargeScaleOnlyDelete) {
  auto memtable = std::make_unique<db::MemTable>(1 /*memtable_version*/);
  const int num_keys = 100000;
  std::string key{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    memtable->Delete(key, 0 /*txn_id*/);
  }

  for (int i = 0; i < num_keys; i++) {
    EXPECT_TRUE(memtable->Get(key, 0 /*txn_id*/).type ==
                db::ValueType::DELETED);
    EXPECT_TRUE(memtable->Get(key, 0 /*txn_id*/).value == std::nullopt);
  }
}

TEST(MemTableTest, LargeScaleDelete) {
  auto memtable = std::make_unique<db::MemTable>(1 /*memtable_version*/);

  const int num_keys = 100000;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    memtable->Put(key, value, 0);
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    memtable->Delete(key, 0);
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    EXPECT_TRUE(memtable->Get(key, 0).type == db::ValueType::DELETED);
    EXPECT_TRUE(memtable->Get(key, 0).value == std::nullopt);
  }
}

TEST(MemTableTest, Iterator) {
  auto memtable = std::make_unique<db::MemTable>(1 /*memtable_version*/);

  const int num_keys = 10;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    memtable->Put(key, value, 0);
  }

  auto iterator = std::make_unique<db::MemTableIterator>(memtable.get());

  int count = 0;
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    key = "key" + std::to_string(count);
    value = "value" + std::to_string(count);
    EXPECT_EQ(iterator->GetKey(), key);
    EXPECT_EQ(iterator->GetValue(), value);
    EXPECT_EQ(iterator->GetType(), db::ValueType::PUT);
    count++;
  }
}

} // namespace kvs