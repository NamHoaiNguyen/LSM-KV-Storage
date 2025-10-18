# Log Structured Merge Tree Key-Value Storage

`LSM_KV_Storage` is a educational project to implement a simple key-value database from scratch, using Log Structured Merge tree(LSM) as the storage engine. The project is inspired by [leveldb](https://github.com/google/leveldb) and [rockdb](https://github.com/facebook/rocksdb).

## Build Configuration

The project uses xmake as the build system. Below is the xmake configuration for building the project and running tests:

1. Compile the project
```bash
cmake -DCMAKE_BUILD_TYPE=Release -B build -S . -G "Unix Makefiles"
make -j$(nproc)
```

2. Debug build
```bash
cmake -DCMAKE_BUILD_TYPE=Debug -B build -S . -G "Unix Makefiles"
make -j$(nproc)
```


2. Run test
```bash
cd build/tests
Run program that have "test_" prefix
```

## Usage

Here is an example demonstrating how to use
```cpp
#include "db/db_impl.h"

#include <iostream>

using namespace kvs;

int main() {
  kvs::db::DBImpl db;
  bool success = db.LoadDB("test");
  if (!success) {
    return 0;
  }

  // Put data
  db.Put("key1", "value1");
  db.Put("thunder", "colossus");
  db.Put("A random key!!!", "random value");

  // Query data;
  auto status = db.Get("key1");
  if (status.value.has_value()) {
    std::cout << "key1: " << status.value.value() << std::endl;
  } else {
    std::cout << "key1 not found" << std::endl;
  }
  status = db.Get("A random key!!!");
  if (status.value.has_value()) {
    std::cout << "A random key!!!: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }
  status = db.Get("thunder");
  if (status.value.has_value()) {
    std::cout << "thunder: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  // Delete key
  db.Delete("thunder");
  status = db.Get("thunder");
  if (status.value.has_value()) {
    std::cout << "thunder: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  // Update a key
  db.Put("key1", "new value updated by key1");
  status = db.Get("key1");
  if (status.value.has_value()) {
    std::cout << "key1: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  return 0;
}
```

## Features && TODO
- [x] SkipList
  - [x] Get/Put/Delete
  - [x] Iterator
- [x] MemTable
  - [x] Iterator
  - [x] Flush to sst
- [x] SST
  - [x] Encode/Decode
  - [x] Cache Eviction for Block and Table
  - [x] Versioning
  - [x] MergeIterator
  - [x] Compact
  - [ ] Bloom Filter
- [ ] MemTable Wal
  - [ ] Sync Wal
  - [ ] Async Wal
  - [ ] Recover
- [x] Manifest
  - [x] Use Manifest to record all operations
  - [x] Recover DB based on Manifest
- [ ] Transaction
  - [ ] MVCC
  - [ ] Isolation Level
    - [ ] Read Uncommitted
    - [ ] Read Committed
    - [ ] Repeatable Read
    - [ ] Serializable
- [x] Config
  - [x] Toml Config

## License
This project is licensed under the MIT License.