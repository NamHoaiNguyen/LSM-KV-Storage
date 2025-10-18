`LSM_KV Storage` is a educational project to implement a simple key-value database from scratch, using Log Structured Merge tree(LSM) as the storage engine. The project is inspired by [leveldb](https://github.com/google/leveldb) and [rockdb](https://github.com/facebook/rocksdb).

# Build Configuration

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

# Usage

Here is an example demonstrating how to use
```
```

# Features && TODO
- [x] SkipList
  - [x] get/put/remove
  - [x] iterator
- [x] MemTable
  - [x] Iterator
  - [x] flush to sst
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

# License
This project is licensed under the MIT License.