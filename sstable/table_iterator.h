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

class TableIterator : public kvs::BaseIterator {
public:
  TableIterator = default;

  ~TableIterator() override = default;

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
};

} // namespace sstable

} // namespace kvs

#endif // DB_TABLE_ITERATOR_H