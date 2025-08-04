#include <gtest/gtest.h>

#include "db/memtable.h"
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