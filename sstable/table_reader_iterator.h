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
class LRUBlockItem;
class LRUTableItem;
class TableReader;

class TableReaderIterator : public kvs::BaseIterator {
public:
  TableReaderIterator(
      const std::vector<std::unique_ptr<BlockReaderCache>> &block_reader_cache,
      std::shared_ptr<LRUTableItem> lru_table_item);

  ~TableReaderIterator();

  // No copy allowed
  TableReaderIterator(const TableReaderIterator &) = delete;
  TableReaderIterator &operator=(TableReaderIterator &) = delete;

  // No move allowed
  TableReaderIterator(TableReaderIterator &&other) = delete;
  TableReaderIterator &operator=(TableReaderIterator &&other) = delete;

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

  const std::vector<std::unique_ptr<BlockReaderCache>> &block_reader_cache_;

  std::weak_ptr<LRUTableItem> lru_table_item_;

  const TableReader *table_reader_;

  std::vector<std::shared_ptr<LRUBlockItem>> list_lru_blocks_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_ITERATOR_H