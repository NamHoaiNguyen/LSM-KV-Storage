#include "sstable/table_reader.h"

#include "io/linux_file.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"

namespace {

uint64_t GetDataEntryOffset(uint64_t offset_section, int entry_index,
                            std::span<const kvs::Byte> buffer) {
  // Starting offset of offset entry at index (entry_index) (th)
  uint64_t offset_entry = offset_section + entry_index * 2 * sizeof(uint64_t);

  const uint64_t data_entry_offset =
      *reinterpret_cast<const uint64_t *>(&buffer[offset_entry]);

  return data_entry_offset;
}

} // namespace

namespace kvs {
constexpr int kDefaultExtraInfoSize = 40; // Bytes
}

namespace kvs {

namespace sstable {

std::unique_ptr<TableReader>
CreateAndSetupDataForTableReader(std::string &&filename, SSTId table_id,
                                 uint64_t file_size) {
  auto table_reader_data = std::make_unique<TableReaderData>();

  table_reader_data->filename = std::move(filename);
  table_reader_data->table_id = table_id;
  table_reader_data->file_size = file_size;
  table_reader_data->read_file_object =
      std::make_unique<io::LinuxReadOnlyFile>(table_reader_data->filename);
  if (!table_reader_data->read_file_object->Open()) {
    return nullptr;
  }

  // Decode block index
  DecodeExtraInfo(table_reader_data.get());

  auto new_table_reader =
      std::make_unique<TableReader>(std::move(table_reader_data));
  return new_table_reader;
}

void DecodeExtraInfo(TableReaderData *table_reader_data) {
  std::array<Byte, kDefaultExtraInfoSize> extra_info_buffer;

  // Get last 40 bytes
  uint64_t start_offset_extra_info =
      table_reader_data->file_size - kDefaultExtraInfoSize - 1;

  ssize_t bytes_read = table_reader_data->read_file_object->RandomRead(
      extra_info_buffer, start_offset_extra_info);
  if (bytes_read < 0) {
    return;
  }

  // first 8 bytes contains info of total block entries in table
  uint64_t total_block_entries =
      *reinterpret_cast<uint64_t *>(&extra_info_buffer[0]);
  // byte 8 - 15 contains starting offset of meta section
  uint64_t starting_meta_section_offset =
      *reinterpret_cast<uint64_t *>(&extra_info_buffer[8]);
  // byte 16 - 23 contains length of meta section
  uint64_t meta_section_length =
      *reinterpret_cast<uint64_t *>(&extra_info_buffer[16]);
  // byte 24- 31 contains min transaction id
  table_reader_data->min_transaction_id =
      *reinterpret_cast<uint64_t *>(&extra_info_buffer[24]);
  // byte 32 - 40 contain max transcation id
  table_reader_data->max_transaction_id =
      *reinterpret_cast<uint64_t *>(&extra_info_buffer[32]);

  // Fill block index info into block_index_
  FetchBlockIndexInfo(total_block_entries, starting_meta_section_offset,
                      meta_section_length, table_reader_data);
}

void FetchBlockIndexInfo(uint64_t total_block_entries,
                         uint64_t starting_meta_section_offset,
                         uint64_t meta_section_length,
                         TableReaderData *table_reader_data) {
  assert(total_block_entries > 0 && total_block_entries < ULLONG_MAX);
  assert(starting_meta_section_offset > 0 &&
         starting_meta_section_offset < ULLONG_MAX);
  assert(meta_section_length > 0 && meta_section_length < ULLONG_MAX);

  std::vector<Byte> block_index_buffer(meta_section_length, 0);
  ssize_t bytes_read = table_reader_data->read_file_object->RandomRead(
      block_index_buffer, starting_meta_section_offset);
  if (bytes_read < 0) {
    return;
  }

  uint64_t starting_offset = 0;

  for (int i = 0; i < total_block_entries; i++) {
    // First 4 bytes contain info smallest key length
    const uint32_t smallest_key_length = *reinterpret_cast<const uint32_t *>(
        &block_index_buffer[starting_offset]);

    // Move block_index_current_offset to starting offset of key
    starting_offset += sizeof(uint32_t);

    // Get smallest key based on smallest key length info
    std::string_view block_smallest_key(
        reinterpret_cast<const char *>(&block_index_buffer[starting_offset]),
        smallest_key_length);

    // Update offset to point to starting index of largest key
    starting_offset += block_smallest_key.size();

    // Next 4 bytes contain info block largest key
    uint32_t largest_key_length = *reinterpret_cast<const uint32_t *>(
        &block_index_buffer[starting_offset]);

    // Move block_index_current_offset to starting offset of value
    starting_offset += sizeof(uint32_t);

    // Get largest key based on largest key length info
    std::string_view block_largest_key(
        reinterpret_cast<const char *>(&block_index_buffer[starting_offset]),
        largest_key_length);

    // Update offset to point to starting offset of corresponding data block
    // (8B)
    starting_offset += block_largest_key.size();

    // Next 8 bytes contain starting offset of corresponding data block
    const uint64_t block_starting_offset = *reinterpret_cast<const uint64_t *>(
        &block_index_buffer[starting_offset]);

    // Update offset to point to starting offset of corresponding data block
    // (8B)
    starting_offset += sizeof(uint64_t);

    // Next 8 bytes contain starting offset of corresponding data block
    const uint64_t block_length = *reinterpret_cast<const uint64_t *>(
        &block_index_buffer[starting_offset]);

    // Update offset to point to starting offset of next block index
    starting_offset += sizeof(uint64_t);

    // Cache block index info
    table_reader_data->block_index.emplace_back(
        block_smallest_key, block_largest_key, block_starting_offset,
        block_length);
  }
}

TableReader::TableReader(std::unique_ptr<TableReaderData> table_reader_data)
    : filename_(std::move(table_reader_data->filename)),
      table_id_(table_reader_data->table_id),
      file_size_(table_reader_data->file_size),
      min_transaction_id_(table_reader_data->min_transaction_id),
      max_transaction_id_(table_reader_data->max_transaction_id),
      block_index_(std::move(table_reader_data->block_index)),
      read_file_object_(std::move(table_reader_data->read_file_object)) {}

db::GetStatus
TableReader::SearchKey(std::string_view key, TxnId txn_id,
                       const sstable::BlockReaderCache *block_reader_cache,
                       const TableReader *table_reader) const {
  assert(block_reader_cache);
  auto [block_offset, block_size] = GetBlockOffsetAndSize(key);

  return block_reader_cache->GetKeyFromBlockCache(
      key, txn_id, {table_id_, block_offset}, block_size, table_reader);
}

std::pair<BlockOffset, BlockSize>
TableReader::GetBlockOffsetAndSize(std::string_view key) const {
  // Find the block that have smallest largest key that >= key
  int left = 0;
  int right = block_index_.size();

  while (left < right) {
    int mid = left + (right - left) / 2;
    if (block_index_[mid].GetLargestKey() >= key) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  BlockOffset block_offset = block_index_[right].GetBlockStartOffset();
  BlockSize block_size = block_index_[right].GetBlockSize();

  return {block_offset, block_size};
}

std::unique_ptr<BlockReader>
TableReader::CreateAndSetupDataForBlockReader(BlockOffset offset,
                                              uint64_t block_size) const {
  if (offset < 0) {
    return nullptr;
  }

  auto block_reader_data = std::make_unique<BlockReaderData>(block_size);
  ssize_t bytes_read =
      read_file_object_->RandomRead(block_reader_data->buffer, offset);
  if (bytes_read < 0) {
    return nullptr;
  }

  int64_t last_block_offset = block_reader_data->buffer.size() - 1;
  // 16 last bytes of lock contain metadata info(num entries + starting offset
  // of offset section)
  block_reader_data->total_data_entries = *reinterpret_cast<uint64_t *>(
      &block_reader_data->buffer[last_block_offset - 15]);
  block_reader_data->offset_section = *reinterpret_cast<uint64_t *>(
      &block_reader_data->buffer[last_block_offset - 7]);

  for (uint64_t i = 0; i < block_reader_data->total_data_entries; i++) {
    block_reader_data->data_entries_offset_info.emplace_back(GetDataEntryOffset(
        block_reader_data->offset_section, i, block_reader_data->buffer));
  }

  // Create new blockreader
  auto new_block_reader =
      std::make_unique<BlockReader>(std::move(block_reader_data));

  return new_block_reader;
}

uint64_t TableReader::GetFileSize() const { return file_size_; }

const std::vector<BlockIndex> &TableReader::GetBlockIndex() const {
  return block_index_;
}

} // namespace sstable

} // namespace kvs