#ifndef DB_BASE_MEMTABLE_H
#define DB_BASE_MEMTABLE_H

#include "common/macros.h"

namespace kvs {

class BaseMemtable {
  public:
    virtual ~BaseMemtable() = default;

    void BatchGet() = 0;

    void Get() = 0;

    void BatchPut() = 0;

    void Put() = 0;

  private:
};

} // namespace kvs

#endif // DB_BASE_MEMTABLE_H