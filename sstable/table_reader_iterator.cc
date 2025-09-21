#include "sstable/table_reader_iterator.h"

#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/block_reader_iterator.h"
#include "sstable/table_reader.h"

#include <iostream>

namespace kvs {

namespace sstable {

TableReaderIterator::TableReaderIterator(
    const BlockReaderCache *block_reader_cache, const TableReader *table_reader)
    : block_reader_iterator_(nullptr), current_block_offset_index_(0),
      block_reader_cache_(block_reader_cache), table_reader_(table_reader) {
  assert(block_reader_cache_ && table_reader_);
}

std::string_view TableReaderIterator::GetKey() {
  return block_reader_iterator_->GetKey();
}

std::string_view TableReaderIterator::GetValue() {
  return block_reader_iterator_->GetValue();
}

db::ValueType TableReaderIterator::GetType() {
  return block_reader_iterator_->GetType();
}

TxnId TableReaderIterator::GetTransactionId() {
  return block_reader_iterator_->GetTransactionId();
}

bool TableReaderIterator::IsValid() {
  return 0 <= current_block_offset_index_ &&
         current_block_offset_index_ < table_reader_->block_index_.size();
}

void TableReaderIterator::Next() {
  if (!block_reader_iterator_) {
    return;
  }

  block_reader_iterator_->Next();
  if (block_reader_iterator_->IsValid()) {
    // There is still data entry in block to iterate
    // block_reader_iterator_->Next();
    return;
  }

  // Else, move to next block
  current_block_offset_index_++;
  if (!IsValid()) {
    return;
  }

  if (current_block_offset_index_ >= table_reader_->block_index_.size()) {
    std::cout << "namnh debug TableReaderIterator::Next" << std::endl;
  }

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
}

void TableReaderIterator::Prev() {
  if (!block_reader_iterator_) {
    return;
  }

  block_reader_iterator_->Prev();
  if (block_reader_iterator_->IsValid()) {
    // There is still data entry in block to iterate
    return;
  }

  // Else, move to next block
  current_block_offset_index_--;
  if (!IsValid()) {
    return;
  }

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
}

void TableReaderIterator::Seek(std::string_view key) {
  // Find the block that have smallest largest key that >= key
  CreateNewBlockReaderIterator(table_reader_->GetBlockOffsetAndSize(key));
}

void TableReaderIterator::SeekToFirst() {
  current_block_offset_index_ = 0;

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
}

void TableReaderIterator::SeekToLast() {
  current_block_offset_index_ = table_reader_->block_index_.size() - 1;

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
}

std::pair<BlockOffset, BlockSize>
TableReaderIterator::GetBlockOffsetAndSizeBaseOnIndex() {
  BlockOffset block_offset =
      table_reader_->block_index_[current_block_offset_index_]
          .GetBlockStartOffset();
  BlockSize block_size =
      table_reader_->block_index_[current_block_offset_index_].GetBlockSize();

  return {block_offset, block_size};
}

void TableReaderIterator::CreateNewBlockReaderIterator(
    std::pair<BlockOffset, BlockSize> block_info) {
  SSTId table_id = table_reader_->table_id_;
  // Look up block in cache
  const BlockReader *block_reader =
      block_reader_cache_->GetBlockReader({table_id, block_info.first});
  if (block_reader) {
    // if had already been in cache
    block_reader_iterator_.reset(new BlockReaderIterator(block_reader));
    return;
  }

  // If not, create new blockreader and load data from disk
  std::unique_ptr<BlockReader> new_block_reader =
      table_reader_->CreateAndSetupDataForBlockReader(block_info.first,
                                                      block_info.second);

  // Create new blockreaderiterator object
  block_reader_iterator_.reset(new BlockReaderIterator(new_block_reader.get()));

  // Insert new blockreader into cache
  block_reader_cache_->AddNewBlockReader({block_info.first, block_info.second},
                                         std::move(new_block_reader));
}

} // namespace sstable

} // namespace kvs