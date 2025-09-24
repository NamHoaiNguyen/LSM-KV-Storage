#ifndef SSTABLE_TABLE_READER_ITERATOR_H
#define SSTABLE_TABLE_READER_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

// libC++
#include <cassert>
#include <memory>

namespace kvs {

namespace sstable {

class BlockReaderCache;
class BlockReaderIterator;
class TableReader;

class TableReaderIterator : public kvs::BaseIterator {
public:
  TableReaderIterator(const BlockReaderCache *block_reader_cache,
                      const TableReader *table_reader);

  ~TableReaderIterator() = default;

  // No copy allowed
  TableReaderIterator(const TableReaderIterator &) = delete;
  TableReaderIterator &operator=(TableReaderIterator &) = delete;

  // Move constructor/assignment
  TableReaderIterator(TableReaderIterator &&other) {
    current_block_offset_index_ = other.current_block_offset_index_;
    block_reader_iterator_ = std::move(other.block_reader_iterator_);
    block_reader_cache_ = other.block_reader_cache_;
    table_reader_ = other.table_reader_;
    // other.block_reader_cache_ = nullptr;
    // other.table_reader_ = nullptr;
  }

  TableReaderIterator &operator=(TableReaderIterator &&other) {
    current_block_offset_index_ = other.current_block_offset_index_;
    block_reader_iterator_ = std::move(other.block_reader_iterator_);
    block_reader_cache_ = other.block_reader_cache_;
    table_reader_ = other.table_reader_;
    // other.block_reader_cache_ = nullptr;
    // other.table_reader_ = nullptr;
    return *this;
  }

  std::string_view GetKey() override;

  std::string_view GetValue() override;

  db::ValueType GetType() override;

  TxnId GetTransactionId() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  // Jump  to and load first block in table
  void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  std::pair<BlockOffset, uint64_t> GetBlockOffsetAndSizeBaseOnIndex();

  void
  CreateNewBlockReaderIterator(std::pair<BlockOffset, BlockSize> block_info);

  uint64_t current_block_offset_index_;

  std::unique_ptr<BlockReaderIterator> block_reader_iterator_;

  const BlockReaderCache *block_reader_cache_;

  const TableReader *table_reader_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_ITERATOR_H