#ifndef SSTABLE_BLOCK_READER_ITERATOR_H
#define SSTABLE_BLOCK_READER_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

// libC++
#include <cassert>
#include <optional>

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
  BlockReaderIterator(BlockReaderIterator &&) = default;
  BlockReaderIterator &operator=(BlockReaderIterator &&) = default;

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

private:
  std::optional<uint64_t> GetCurrentDataEntryOffset();

  const BlockReader *block_reader_;

  uint64_t current_offset_index_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READER_ITERATOR_H