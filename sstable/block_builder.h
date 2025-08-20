#ifndef SSTABLE_BLOCK_BUILDER_H
#define SSTABLE_BLOCK_BUILDER_H

#include "common/macros.h"

// libC++
#include <string_view>

namespace kvs {

class BlockBuilder {
public:
  BlockBuilder() = default;

  ~BlockBuilder() = default;

  // No copy allowed
  BlockBuilder(const BlockBuilder &) = default;
  BlockBuilder &operator=(BlockBuilder &) = default;

  // Move constructor/assignment
  BlockBuilder(BlockBuilder &&) = default;
  BlockBuilder &operator=(BlockBuilder &&) = default;

  void Add(std::string_view key, std::string_view value, TxnId txn_id);

  // Finish building the block
  void Finish();

  // Clear data of block to reuse
  void Reset();

  size_t GetBlockSize() const;

private:
  bool is_finished_;

  uint64_t num_entries_;

  uint64_t data_offset_;

  uint64_t index_offset_;

  // TODO(namnh) : Is there any way to avoid copying?
  std::vector<Byte> buffer_;
};

} // namespace kvs

#endif // SSTABLE_BLOCK_BUILDER_H