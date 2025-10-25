# Log-Structured Merge Tree (LSM) – High-Level Design

## Introduction
`LSM_KV_Storage` is a educational project to implement a simple key-value database from scratch, using Log Structured Merge tree(LSM) as the storage engine.
It is a C++ library to store keys and values and support points lookup(Range lookup will be supported in the fure)
The project is heavily inspired by [leveldb](https://github.com/google/leveldb) and [rockdb](https://github.com/facebook/rocksdb).

## High Level Architecture
LSM_KV_Storage is a storage engine RocksDB is a storage engine library of key-value store interface where keys and values are string. It organizes all data in sorted order and the common operations are Get(key), Put(key) and Delete(key)

The three core components of the DB are memtables, SST files and logfile. The memtable is an in-memory data structure. When the memtable fills up, it is flushed to a SST file. The data in an SST file is roted based on key

[Design Overview](design.png)

## Features

### Memtable
A MemTable is an in-memory, sorted data structure used to store recent writes before they’re flushed to disk. When implemented with a SkipList, it keeps key-value pairs in sorted order, enabling fast inserts, lookups, and iteration. Each write is first logged to a Write-Ahead Log (WAL) for durability, then inserted into the SkipList. Once the MemTable grows beyond a threshold, it’s frozen and flushed to disk as an immutable SSTable. SkipLists are chosen for their simplicity, predictable O(log n) performance, and efficient ordered traversal, making them ideal for managing high-throughput, write-optimized storage engines.

### SST
An SST (Sorted String Table) is an immutable, on-disk file used to store sorted key-value data. Each SST is created by flushing a MemTable and is organized into blocks containing data, indexes for fast lookups. Because SSTs are sorted and immutable, they enable efficient range scans, quick merges during compaction, and consistent crash recovery. SSTs form the foundational storage layer of the LSM tree, providing durable, structured, and query-efficient persistence for large-scale data.

### Compaction
Compaction is a background process in LSM-based databases that merges and reorganizes SSTables on disk to maintain sorted order, remove obsolete data, and reclaim space. As new data is flushed from memory, compaction combines overlapping files, ensuring efficient reads and balanced storage levels. It’s essential for keeping the database fast, compact, and consistent over time.

### Manifest
The Manifest is a critical metadata file that records the state and organization of all SSTables on disk. It tracks which files exist in each level, their key ranges, and sequence numbers, allowing the database to reconstruct its state after a restart or crash. Every time a new SSTable is created or a compaction modifies file layout, a new record is appended to the Manifest. This append-only design ensures durability and consistency of metadata without rewriting the entire file. In essence, the Manifest acts as the authoritative index of the database’s on-disk structure, guiding recovery, compaction, and query operations.

### Caching

#### SST(Table) Cache
The TableCache is responsible for managing access to SSTable files on disk. It caches recently opened tables, avoiding the overhead of repeatedly opening files and reading their metadata. When a read request targets a specific SSTable, the TableCache quickly returns a handle to the already-open file, along with its index and filter blocks, enabling faster lookups. By keeping a limited number of open file handles and metadata in memory, the TableCache reduces filesystem calls and improves read performance, especially for workloads that frequently access a subset of SSTables.

#### Block Cache
The BlockCache is an in-memory cache that stores frequently accessed data and index blocks from SSTables to reduce disk reads and improve read performance. When a query retrieves a block, it is stored in the cache so subsequent lookups for the same data can be served directly from memory. In this project, the BlockCache is enhanced with sharding support, meaning the cache is divided into multiple independent shards, each managing a portion of the data. Sharding reduces lock contention and allows concurrent access from multiple threads, improving scalability and throughput on multi-core systems