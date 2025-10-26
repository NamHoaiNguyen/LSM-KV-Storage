## MemTable Construction Principle

Let view overall architecture design

[Architecture](memtable.png)

The MemTable stores key-value pairs in memory, using a SkipList as the underlying data structure. It is divided into two parts: current_table and frozen_table. The current_table is read-writable but primarily handles new writes, while the frozen_table is read-only and holds immutable data that has already been written. When the current_table reaches its capacity threshold, it is converted into a new frozen_table instance.

[Frozen flow](frozen_memtable_flow.png)

This design enhances concurrency. Writes are directed only to the current_table, while reads can access both the current_table and existing frozen_tables. Without this separation, reads and writes would compete on the same large structure, leading to contention and reduced performance. By isolating active writes from read-only data, the system allows concurrent queries and writes, significantly improving overall throughput.

The most important options that affect memtable behavior are:
* LSM_PER_MEM_SIZE_LIMIT: defines the maximum size of a single in-memory MemTable (default 32 MB). When a MemTable reaches this limit, it becomes immutable and is scheduled for flushing to disk as an SSTable. A new writable MemTable is then created to handle subsequent writes.

* MAX_IMMUTABLE_MEMTABLES_IN_MEMORY: sets the maximum number of immutable MemTables that can remain in memory at once (default 4). When this limit is reached, a background thread will flush those immutable memtables to disk

## Skiplist MemTable
The default implementation of memtable is based on skiplist. SkipList is chosen for its simplicity, predictable O(log n) performance, and efficient ordered traversal, making them ideal for managing high-throughput, write-optimized storage engines.