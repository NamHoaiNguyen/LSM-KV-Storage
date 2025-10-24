#ifndef SSTABLE_BLOCK_READER_ITERATOR_H
#define SSTABLE_BLOCK_READER_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

// libC++
#include <cassert>
#include <memory>

namespace kvs {

namespace sstable {

class BlockReader;
class LRUBlockItem;

class BlockReaderIterator : public kvs::BaseIterator {
public:
  explicit BlockReaderIterator(std::shared_ptr<LRUBlockItem> block_reader);

  ~BlockReaderIterator();

  // No copy allowed
  BlockReaderIterator(const BlockReaderIterator &) = delete;
  BlockReaderIterator &operator=(BlockReaderIterator &) = delete;

  // No move allowed
  BlockReaderIterator(BlockReaderIterator &&other) = delete;
  BlockReaderIterator &operator=(BlockReaderIterator &&other) = delete;

  std::string_view GetKey() override;

  std::string_view GetValue() override;

  db::ValueType GetType() override;

  TxnId GetTransactionId() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  // Find the first key that >= key in block
  void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

  friend class TableReaderIterator;

private:
  std::optional<uint64_t> GetCurrentDataEntryOffset();

  // std::weak_ptr<LRUBlockItem> lru_block_item_;
  std::shared_ptr<LRUBlockItem> lru_block_item_;

  const BlockReader *block_reader_;

  // Contain index of block_index_ data member in BlockReader
  uint64_t current_offset_index_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READER_ITERATOR_H