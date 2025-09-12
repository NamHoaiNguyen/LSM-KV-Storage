#ifndef DB_TABLE_ITERATOR_H
#define DB_TABLE_ITERATOR_H

#include "common/base_iterator.h"
#include "common/macros.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace kvs {

namespace sstable {

class BlockReader;

class BlockIterator : public kvs::BaseIterator {
public:
  BlockIterator();

  ~BlockIterator() override = default;

  std::string_view GetKey() override;

  std::optional<std::string_view> GetValue() override;

  ValueType GetType() override;

  TxnId GetTransactionId() override;

  bool IsValid() override;

  void Next() override;

  void Prev() override;

  void Seek(std::string_view key) override;

  void SeekToFirst() override;

  void SeekToLast() override;

private:
  std::unique_ptr<BlockReader> block_reader_;
};

} // namespace sstable

} // namespace kvs

#endif // DB_TABLE_ITERATOR_H