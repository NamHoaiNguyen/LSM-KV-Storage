#include <gtest/gtest.h>

#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/skiplist.h"

#include <memory>

TEST(MemTableTest, BasicOperations) {
  auto memtable = std::make_unique<kvs::MemTable>();

  memtable->Put("k1", "v1", 0);
  EXPECT_TRUE(memtable->Get("k1", 0).has_value());
  EXPECT_EQ(memtable->Get("k1", 0).value(), "v1");

  // update
  memtable->Put("k1", "v2", 0);
  EXPECT_TRUE(memtable->Get("k1", 0).has_value());
  EXPECT_EQ(memtable->Get("k1", 0).value(), "v2");

  // delete
  EXPECT_TRUE(memtable->Delete("k1", 0));
}

TEST(MemTableTest, LargeScalePutAndGet) {
  auto memtable = std::make_unique<kvs::MemTable>();

  const int num_keys = 100000;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    memtable->Put(key, value, 0);
  }

  std::optional<std::string> val;
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    val = memtable->Get(key, 0);
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), value);
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

    val = memtable->Get(key, 0);
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), value);
  }
}

TEST(MemTableTest, LargeScaleDelete) {
  auto memtable = std::make_unique<kvs::MemTable>();

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

    EXPECT_TRUE(memtable->Delete(key, 0));
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    EXPECT_FALSE(memtable->Get(key, 0).has_value());
  }
}

TEST(MemTableTest, Iterator) {
  auto memtable = std::make_unique<kvs::MemTable>();

  const int num_keys = 10;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    memtable->Put(key, value, 0);
  }

  auto iterator = std::make_unique<kvs::MemTableIterator>(memtable.get());

  int count = 0;
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    key = "key" + std::to_string(count);
    value = "value" + std::to_string(count);
    EXPECT_EQ(iterator->GetKey(), key);
    EXPECT_EQ(iterator->GetValue(), value);
    count++;
  }
}