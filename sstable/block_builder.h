#ifndef SSTABLE_BLOCK_BUILDER_H
#define SSTABLE_BLOCK_BUILDER_H

// libC++
#include <string_view>

namespace kvs {

class BlockBuilder {
public:
  BlockBuilder();

  ~BlockBuilder();

  // No copy allowed
  BlockBuilder(const BlockBuilder &) = default;
  BlockBuilder &operator=(BlockBuilder &) = default;

  // Move constructor/assignment
  BlockBuilder(BlockBuilder &&) = default;
  BlockBuilder &operator=(BlockBuilder &&) = default;

  void Add(std::string_view key, std::string_view value);

  // Finish building the block
  void Finish();

  // Clear data of block to reuse
  void Reset();

private:
};

} // namespace kvs

#endif // SSTABLE_BLOCK_BUILDER_H