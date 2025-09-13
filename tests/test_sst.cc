#include <gtest/gtest.h>

#include "common/macros.h"
#include "db/base_memtable.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/status.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/linux_file.h"
#include "sstable/block_builder.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"
#include "sstable/table.h"

#include <cmath>
#include <memory>

namespace kvs {

namespace sstable {

std::vector<Byte> data_encoded = {
    // Data section
    // Entry 1
    0,                            // value_type = 0(not_deleted)
    5, 0, 0, 0,                   // key_len = 5
    'a', 'p', 'p', 'l', 'e',      // key
    6, 0, 0, 0,                   // value_len = 6
    'v', 'a', 'l', 'u', 'e', '1', // value
    // 0x39, 0x30, 0, 0, 0, 0, 0, 0, // transaction_id = 12345
    0, 0, 0, 0, 0, 0, 0, 0, // transaction_id = 0
    // Entry 2
    0,                                 // value_type = 0(not_deleted)
    5, 0, 0, 0,                        // key_len = 5
    'a', 'p', 'p', 'l', 'y',           // key
    7, 0, 0, 0,                        // value_len = 7
    's', 'u', 'c', 'c', 'e', 's', 's', // value
    // 0x94, 0x26, 0, 0, 0, 0, 0, 0,      // transaction_id = 9876
    0, 0, 0, 0, 0, 0, 0, 0, // transaction_id = 0

    // Entry 3
    0,                                      // value_type = 0(not_deleted)
    8, 0, 0, 0,                             // key_len = 8
    'c', 'o', 'l', 'o', 's', 's', 'u', 's', // key
    7, 0, 0, 0,                             // value_len = 7
    't', 'h', 'u', 'n', 'd', 'e', 'r',      // value
    // 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0      // transaction_id = 2^32-1
    0, 0, 0, 0, 0, 0, 0, 0, // transaction_id = 0

};

std::vector<Byte> buffer_encoded = {
    // Offset entry 0
    0, 0, 0, 0, 0, 0, 0, 0,    // start_offset of data entry 0
    0x1C, 0, 0, 0, 0, 0, 0, 0, // length of data entry 0
    // Offset entry 1
    0x1C, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 1
    0x1D, 0, 0, 0, 0, 0, 0, 0, // length of data entry 1
    // Offset entry 2
    0x39, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 2
    0x20, 0, 0, 0, 0, 0, 0, 0, // length of data entry 2
};

std::vector<Byte> block_index_buffer_encoded = {

    5,    0,   0,   0,                       // length of first key(4B)
    'a',  'p', 'p', 'l', 'e',                // first key
    8,    0,   0,   0,                       // length of last key(4B)
    'c',  'o', 'l', 'o', 's', 's', 'u', 's', // last key
    0,    0,   0,   0,   0,   0,   0,   0,   // starting offset of block((8B))
    0x86, 0,   0,   0,   0,   0,   0,   0,   // block size(8B) (data + metadata)
};

TEST(TableTest, BasicEncode) {
  auto config = std::make_unique<db::Config>(true /*is_testing*/);
  config->LoadConfig();

  std::string filename = "/home/hoainam/self/biggg/lsm-kv-storage/data/1.sst";
  uint64_t table_id = 1;
  auto table = std::make_unique<sstable::Table>(std::move(filename), table_id,
                                                config.get());
  table->GetWriteOnlyFileObject()->Open();

  std::string key1 = "apple";
  std::string value1 = "value1";
  TxnId txn_id = 0;
  table->AddEntry(key1, value1, txn_id, db::ValueType::PUT);

  key1 = "apply";
  value1 = "success";
  txn_id = 0;
  table->AddEntry(key1, value1, txn_id, db::ValueType::PUT);

  key1 = "colossus";
  value1 = "thunder";
  txn_id = 0;
  table->AddEntry(key1, value1, txn_id, db::ValueType::PUT);

  // EXPECT_EQ(table->block_data_->data_buffer_, encoded);
  // EXPECT_EQ(table->block_data_->offset_buffer_, buffer_encoded);
  // EXPECT_EQ(table->block_index_->buffer_, block_index_buffer_encoded);
  EXPECT_TRUE(
      std::ranges::equal(table->GetBlockData()->GetDataView(), data_encoded));

  table->Finish();
}

TEST(TableTest, CreateTable) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB();
  const db::Config *const config = db->GetConfig();
  const int nums_elems = 10000000;

  size_t memtable_size = 0;
  std::string key, value, smallest_key, largest_key;
  for (int i = 0; i < nums_elems; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    if (smallest_key.empty()) {
      smallest_key = key;
    }
    if (key > largest_key) {
      largest_key = key;
    }

    db->Put(key, value, 0 /*txn_id*/);
    memtable_size += key.size() + value.size();
    if (memtable_size >= config->GetPerMemTableSizeLimit()) {
      break;
    }
  }

  // Force creating a new sst
  db->ForceFlushMemTable();

  const std::vector<std::vector<std::shared_ptr<db::SSTMetadata>>>
      &level_sst_info =
          db->GetVersionManager()->GetLatestVersion()->GetSstMetadata();
  EXPECT_EQ(level_sst_info[0].size(), 1);

  EXPECT_EQ(level_sst_info[0][0]->smallest_key, smallest_key);
  EXPECT_EQ(level_sst_info[0][0]->largest_key, largest_key);
  EXPECT_EQ(level_sst_info[0][0]->sst_id, 1);
  EXPECT_EQ(level_sst_info[0][0]->level, 0);
  EXPECT_TRUE(level_sst_info[0][0]->table_->GetBlockIndex().size() != 0);
}

} // namespace sstable

} // namespace kvs