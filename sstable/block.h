#ifndef SSTABLE_BLOCK_H
#define SSTABLE_BLOCK_H

#include "common/macros.h"

namespace kvs {
/*
Block data format
---------------------------------------------------------------------------------------------------------------------------
|        Data Section       |         Offset Section      |                             Extra                             |
---------------------------------------------------------------------------------------------------------------------------
| Entry #1 | ... | Entry #N | Offset #1 | ... | Offset #N | num_of_elements, offset data section, offset of offset section|
---------------------------------------------------------------------------------------------------------------------------

Extra size(24 bytes, starting from end of block)

Data section format
--------------------------------------------------------------------------
|                           Entry #1                               | ... |
--------------------------------------------------------------------------
| key_len (2B) | key | value_len (2B) | value | transaction_id(8B) | ... |
--------------------------------------------------------------------------
*/

struct DataEntry {
  std::string_view key;

  uint16_t key_len;

  std::string_view value;

  uint16_t value_len;

  TxnId txn_id;
};

// Mostly used for read path
class Block {
public:
  Block();
  explicit Block(std::span<const uint8_t> data);

  ~Block();

  // No copy allowed
  Block(const Block &) = delete;
  Block &operator=(Block &) = delete;

  void Decode();

  void Encode();

private:
  std::span<const uint8_t> data_;

  std::span<const uint16_t> offset_;
};

} // namespace kvs

#endif // SSTABLE_BLOCK_H