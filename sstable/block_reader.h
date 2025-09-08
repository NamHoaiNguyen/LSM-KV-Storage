#ifndef SSTABLE_BLOCK_READAER_H
#define SSTABLE_BLOCK_READAER_H

#include "common/macros.h"
#include "db/status.h"

// libC++
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace kvs {

namespace io {
class ReadOnlyFile;
}

namespace sstable {

class BlockReader {
public:
  BlockReader(std::string_view filename,
              std::shared_ptr<io::ReadOnlyFile> read_file_object);
  ~BlockReader() = default;

  // No copy allowed
  BlockReader(const BlockReader &) = delete;
  BlockReader &operator=(BlockReader &) = delete;

  // Move constructor/assignment
  BlockReader(BlockReader &&) = default;
  BlockReader &operator=(BlockReader &&) = default;

  db::GetStatus SearchKey(uint64_t offset, uint64_t size, std::string_view key,
                          TxnId txn_id);

private:
  uint64_t GetDataEntryOffset(std::span<const Byte> buffer,
                              const uint64_t offset_section,
                              const int entry_index);

  db::ValueType GetValueTypeFromDataEntry(std::span<const Byte> buffer_view,
                                          uint64_t data_entry_offset);

  std::pair<std::string_view, std::string_view>
  GetKeyValueFromDataEntry(std::span<const Byte> buffer_view,
                           uint64_t data_entry_offset);

  std::vector<Byte> buffer_;

  std::shared_ptr<io::ReadOnlyFile> read_file_object_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READAER_H