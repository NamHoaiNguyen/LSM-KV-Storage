#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "db/base_memtable.h"
#include "common/macros.h"

#include <memory>

namespace kvs {

class SkipList;

class Memtable : public BaseMemtable {
  public:
    Memtable();

    ~Memtable() override;

    void BatchGet() override;

    void Get() override;

    void BatchPut() override;

    void Put() override;

  private:
    enum class {

    }

    std::unique_ptr<SkipList> skip_list_;
};

} // namespace kvs

#endif // DB_MEMTABLE_H