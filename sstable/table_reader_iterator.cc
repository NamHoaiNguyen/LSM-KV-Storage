#include "sstable/table_reader_iterator.h"

#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/block_reader_iterator.h"
#include "sstable/lru_block_item.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

// TableReaderIterator::TableReaderIterator(
//     const BlockReaderCache *block_reader_cache,
//     std::shared_ptr<LRUTableItem> lru_table_item)
//     : block_reader_iterator_(nullptr), current_block_offset_index_(0),
//       lru_table_item_(lru_table_item),
//       block_reader_cache_(block_reader_cache) {
//   table_reader_ = lru_table_item_.lock()->GetTableReader();
//   assert(block_reader_cache_ && lru_table_item_ && table_reader_);
// }

TableReaderIterator::TableReaderIterator(
    const std::vector<std::unique_ptr<BlockReaderCache>> &block_reader_cache,
    std::shared_ptr<LRUTableItem> lru_table_item)
    : block_reader_iterator_(nullptr), current_block_offset_index_(0),
      lru_table_item_(lru_table_item), block_reader_cache_(block_reader_cache) {
  table_reader_ = lru_table_item_.lock()->GetTableReader();
  assert(table_reader_);
}

TableReaderIterator::~TableReaderIterator() {
  if (auto tmp_ptr = lru_table_item_.lock()) {
    tmp_ptr->Unref();
  }
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
    return;
  }

  // Else, move to next block
  current_block_offset_index_++;
  if (!IsValid()) {
    return;
  }

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
  // Each time new block reader iterator is created, move "pointer" to the first
  // data entry of block
  block_reader_iterator_->SeekToFirst();
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
  // Each time new block reader iterator is created, move "pointer" to the last
  // data entry of block
  block_reader_iterator_->SeekToLast();
}

void TableReaderIterator::Seek(std::string_view key) {
  // Find the block that have smallest largest key that >= key
  CreateNewBlockReaderIterator(table_reader_->GetBlockOffsetAndSize(key));
}

void TableReaderIterator::SeekToFirst() {
  current_block_offset_index_ = 0;

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
  block_reader_iterator_->SeekToFirst();
}

void TableReaderIterator::SeekToLast() {
  current_block_offset_index_ = table_reader_->block_index_.size() - 1;

  CreateNewBlockReaderIterator(GetBlockOffsetAndSizeBaseOnIndex());
  block_reader_iterator_->SeekToLast();
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

// void TableReaderIterator::CreateNewBlockReaderIterator(
//     std::pair<BlockOffset, BlockSize> block_info) {
//   SSTId table_id = table_reader_->table_id_;
//   // Look up block in cache
//   std::shared_ptr<LRUBlockItem> block_reader =
//       block_reader_cache_->GetLRUBlockItem({table_id, block_info.first});
//   if (block_reader) {
//     // if had already been in cache
//     block_reader_iterator_.reset(new BlockReaderIterator(block_reader));
//     return;
//   }

//   // If not, create new blockreader and load data from disk
//   std::unique_ptr<BlockReader> new_block_reader =
//       table_reader_->CreateAndSetupDataForBlockReader(block_info.first,
//                                                       block_info.second);
//   if (!new_block_reader) {
//     return;
//   }

//   auto new_lru_block_item = std::make_shared<LRUBlockItem>(
//       std::make_pair(table_id, block_info.first),
//       std::move(new_block_reader), block_reader_cache_);

//   std::shared_ptr<LRUBlockItem> block_reader_inserted =
//       block_reader_cache_->AddNewBlockReaderThenGet(
//           {table_id, block_info.first}, new_lru_block_item,
//           true /*add_then_get*/);

//   block_reader_iterator_.reset(new
//   BlockReaderIterator(block_reader_inserted));
// }

void TableReaderIterator::CreateNewBlockReaderIterator(
    std::pair<BlockOffset, BlockSize> block_info) {
  SSTId table_id = table_reader_->table_id_;
  // Look up block in cache

  // TODO(namnh) : block cache bucket
  // TODO(namnh, IMPORTANCE) : Set value >= 10 cause functor is not invoked when
  // pushing into thread pool
  for (int i = 0; i < block_reader_cache_.size(); i++) {
    std::shared_ptr<LRUBlockItem> block_reader =
        block_reader_cache_[i]->GetLRUBlockItem({table_id, block_info.first});
    if (block_reader) {
      // if had already been in cache
      block_reader_iterator_.reset(new BlockReaderIterator(block_reader));
      return;
    }

    // // If not, create new blockreader and load data from disk
    // std::unique_ptr<BlockReader> new_block_reader =
    //     table_reader_->CreateAndSetupDataForBlockReader(block_info.first,
    //                                                     block_info.second);
    // if (!new_block_reader) {
    //   return;
    // }

    // auto new_lru_block_item = std::make_shared<LRUBlockItem>(
    //     std::make_pair(table_id, block_info.first),
    //     std::move(new_block_reader), block_reader_cache_[i].get());

    // std::shared_ptr<LRUBlockItem> block_reader_inserted =
    //     block_reader_cache_[i]->AddNewBlockReaderThenGet(
    //         {table_id, block_info.first}, new_lru_block_item,
    //         true /*add_then_get*/);

    // block_reader_iterator_.reset(
    //     new BlockReaderIterator(block_reader_inserted));
  }

  // If not, create new blockreader and load data from disk
  std::unique_ptr<BlockReader> new_block_reader =
      table_reader_->CreateAndSetupDataForBlockReader(block_info.first,
                                                      block_info.second);
  if (!new_block_reader) {
    return;
  }

  auto new_lru_block_item = std::make_shared<LRUBlockItem>(
      std::make_pair(table_id, block_info.first), std::move(new_block_reader),
      nullptr /*BlockReaderCache*/);

  block_reader_iterator_.reset(new BlockReaderIterator(new_lru_block_item));
}

} // namespace sstable

} // namespace kvs