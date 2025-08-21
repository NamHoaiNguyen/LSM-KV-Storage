#include <gtest/gtest.h>

#include "common/macros.h"
#include "io/linux_file.h"
#include "sstable/block.h"
#include "sstable/table.h"

#include <cmath>
#include <memory>

namespace kvs {

std::vector<Byte> encoded = {
    // Data section
    // Entry 1
    5, 0, 0, 0,                   // key_len = 5
    'a', 'p', 'p', 'l', 'e',      // key
    6, 0, 0, 0,                   // value_len = 6
    'v', 'a', 'l', 'u', 'e', '1', // value
    0x39, 0x30, 0, 0, 0, 0, 0, 0, // transaction_id = 12345
    // Entry 2
    8, 0, 0, 0,                             // key_len = 8
    'c', 'o', 'l', 'o', 's', 's', 'u', 's', // key
    7, 0, 0, 0,                             // value_len = 7
    't', 'h', 'u', 'n', 'd', 'e', 'r',      // value
    0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0};    // transaction_id = 2^32-1

std::vector<Byte> buffer_encoded = {
    // Offset entry 1
    0, 0, 0, 0, 0, 0, 0, 0,    // start_offset of data entry 1
    0x1B, 0, 0, 0, 0, 0, 0, 0, // length of data entry 1
    // Offset entry 2
    0x2B, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 2
    0x1F, 0, 0, 0, 0, 0, 0, 0, // length of data entry 2
};

TEST(BlockBuilderTest, Encode) {
  // auto block = std::make_unique<Block>();

  std::string key1 = "apple";
  std::string value1 = "value1";
  TxnId txn_id = 12345;
  // // block->AddEntry(key1, value1, txn_id);
  // EXPECT_EQ(block->data_buffer_[0], encoded[0]);

  // key1 = "colossus";
  // value1 = "thunder";
  // txn_id = std::pow(2, 32) - 1;
  // block->AddEntry(key1, value1, txn_id);
  // EXPECT_EQ(block->data_buffer_, encoded);

  // EXPECT_EQ(block->offset_buffer_, buffer_encoded);

  std::string filename =
      "/home/hoainam/self/biggg/lsm-kv-storage/data/000001.sst";
  auto table = std::make_unique<Table>(std::move(filename));
  table->file_object_->Open();

  table->AddEntry(key1, value1, txn_id);
  EXPECT_EQ(table->data_block_->data_buffer_[0], encoded[0]);

  key1 = "colossus";
  value1 = "thunder";
  txn_id = std::pow(2, 32) - 1;
  table->AddEntry(key1, value1, txn_id);
  EXPECT_EQ(table->data_block_->data_buffer_, encoded);

  EXPECT_EQ(table->data_block_->offset_buffer_, buffer_encoded);
}

} // namespace kvs