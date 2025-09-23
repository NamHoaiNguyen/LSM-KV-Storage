#ifndef SSTABLE_BLOCK_READER_ITERATOR_H
#define SSTABLE_BLOCK_READER_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

// libC++
#include <cassert>

namespace kvs {

namespace sstable {

class BlockReader;

class BlockReaderIterator : public kvs::BaseIterator {
public:
  explicit BlockReaderIterator(const BlockReader *block_reader);

  ~BlockReaderIterator() = default;

  // No copy allowed
  BlockReaderIterator(const BlockReaderIterator &) = delete;
  BlockReaderIterator &operator=(BlockReaderIterator &) = delete;

  // Move constructor/assignment
  BlockReaderIterator(BlockReaderIterator &&other) {
    block_reader_ = other.block_reader_;
    current_offset_index_ = other.current_offset_index_;
    other.block_reader_ = nullptr;
  }
  BlockReaderIterator &operator=(BlockReaderIterator &&other) {
    block_reader_ = other.block_reader_;
    current_offset_index_ = other.current_offset_index_;
    other.block_reader_ = nullptr;
    return *this;
  }

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

  const BlockReader *block_reader_;

  // Contain index of block_index_ data member in BlockReader
  uint64_t current_offset_index_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READER_ITERATOR_H