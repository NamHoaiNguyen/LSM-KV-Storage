#include <gtest/gtest.h>

#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/skiplist.h"

#include <memory>

namespace kvs {

struct DBStore {
  // Used to extend lifetime of DBImpl instance
  std::unique_ptr<db::DBImpl> db;
};

TEST(DBTest, BasicOperations) {
  auto db = std::make_unique<db::DBImpl>();

  db->Put("apple", "value1", 12345);
  db->Put("apply", "success", 9876);
  db->Put("zoodiac", "drident", 32);
  db->Put("thunder", "colossus", 4294967295);
  db->Put("tearlament", "reinoheart", 85);

  DBStore *db_store = new DBStore;
  db_store->db = std::move(db);

  // std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // delete db_store;
}

} // namespace kvs