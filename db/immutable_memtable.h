#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "db/base_memtable.h"
#include "common/macros.h"

namespace kvs {

class ImmutableMemtable : public BaseMemtable {
  public:
    ImmutableMemtable();

    ~ImmutableMemtable() override;

    void BatchGet() override;

    void Get() override;

    void BatchPut() override;

    void Put() override;

  private:
};

} // namespace kvs

#endif // DB_IMMUTABLE_MEMTABLE_H