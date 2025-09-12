#include "sstable/block_iterator.h"

#include "sstable/block_reader.h"

namespace kvs {

namespace sstable {

BlockIterator::BlockIterator()
    : block_reader_(std::make_unique<BlockReader>()) {}

std::string_view BlockIterator::GetKey() {}

std::optional<std::string_view> BlockIterator::GetValue() {}

ValueType BlockIterator::GetType() {}

TxnId BlockIterator::GetTransactionId() {}

bool BlockIterator::IsValid() {}

void BlockIterator::Next() {}

void BlockIterator::Prev() {}

void BlockIterator::Seek(std::string_view key) {}

void BlockIterator::SeekToFirst() {}

void BlockIterator::SeekToLast() {}

} // namespace sstable

} // namespace kvs