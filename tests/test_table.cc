#include <gtest/gtest.h>

#include "common/macros.h"
#include "db/status.h"
#include "io/linux_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/table.h"

#include <cmath>
#include <memory>

namespace kvs {

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
  std::string filename =
      "/home/hoainam/self/biggg/lsm-kv-storage/data/000001.sst";
  auto table = std::make_unique<Table>(std::move(filename));
  table->file_object_->Open();

  std::string key1 = "apple";
  std::string value1 = "value1";
  TxnId txn_id = 0;
  table->AddEntry(key1, value1, txn_id, ValueType::PUT);

  key1 = "apply";
  value1 = "success";
  txn_id = 0;
  table->AddEntry(key1, value1, txn_id, ValueType::PUT);

  key1 = "colossus";
  value1 = "thunder";
  txn_id = 0;
  table->AddEntry(key1, value1, txn_id, ValueType::PUT);

  // EXPECT_EQ(table->block_data_->data_buffer_, encoded);
  // EXPECT_EQ(table->block_data_->offset_buffer_, buffer_encoded);
  // EXPECT_EQ(table->block_index_->buffer_, block_index_buffer_encoded);
  EXPECT_TRUE(
      std::ranges::equal(table->GetBlockData()->GetDataView(), data_encoded));

  table->Finish();
}

} // namespace kvs