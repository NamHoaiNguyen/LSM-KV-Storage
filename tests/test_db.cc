#include <gtest/gtest.h>

#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/skiplist.h"

#include <memory>

TEST(MemTableTest, BasicOperations) {
  auto db = std::make_unique<kvs::DBImpl>();

  // db->Put("k1", "v1", 0);
  // EXPECT_TRUE(db->Get("k1", 0).has_value());
  // EXPECT_EQ(db->Get("k1", 0).value(), "v1");

  // // update
  // db->Put("k1", "v22", 0);
  // EXPECT_TRUE(db->Get("k1", 0).has_value());
  // EXPECT_EQ(db->Get("k1", 0).value(), "v22");

  // delete
  // EXPECT_TRUE(memtable->Delete("k1", 0));

  db->Put("apple", "value1", 12345);
  db->Put("apply", "success", 9876);
  db->Put("thunder", "colossus", 4294967295);
}