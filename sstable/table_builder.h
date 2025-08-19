#ifndef SSTABLE_TABLE_BUIDLER_H
#define SSTABLE_TABLE_BUIDLER_H

// libC++
#include <memory>
#include <string_view>

namespace kvs {

class AccessFile;
class BlockBuilder;

class TableBuilder {
public:
  TableBuilder();

  ~TableBuilder() = default;

  // Copy constructor/assignment
  TableBuilder(const TableBuilder &) = default;
  TableBuilder &operator=(TableBuilder &) = default;

  // Move constructor/assignment
  TableBuilder(TableBuilder &&) = default;
  TableBuilder &operator=(TableBuilder &&) = default;

  // Add new key/value pairs to SST
  void Add(std::string_view key, std::string_view value);

  void Finish();

  // Flush block data to disk
  void Flush();

  void WriteBlock();

private:
  std::unique_ptr<AccessFile> file_object_;

  // TODO(namnh) : unique_ptr?
  std::shared_ptr<BlockBuilder> data_block_;

  // TODO(namnh) : unique_ptr?
  std::shared_ptr<BlockBuilder> index_block_;
};

} // namespace kvs

#endif // SSTABLE_TABLE_BUIDLER_H