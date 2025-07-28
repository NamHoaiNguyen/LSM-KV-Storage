#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "db/base_memtable.h"
#include "common/macros.h"

#include <memory>
#include <string_view>

namespace kvs {

class SkipList;

class Memtable : public BaseMemtable {
  public:
    Memtable();

    ~Memtable() override;

    void BatchGet() override;

    void Get(std::string_view key) override;

    void BatchPut() override;

    void Put(std::string_view key, std::string_view value, TxnId txn_id) override;

    void Delete() override;

  private:
    enum class {

    }

    // TODO(namnh) : unique_ptr or shared_ptr
    std::unique_ptr<SkipList> table_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H