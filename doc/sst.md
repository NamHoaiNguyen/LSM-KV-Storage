## Sorted-String Table(SST) Construction Principle

## Architecture

The SSTable (Sorted String Table) is an immutable, persistent file format used to store sorted key-value pairs on disk. Each SSTable represents a snapshot of a segment of data that was once held in memory (in the MemTable) and later flushed to disk. Because SSTables are immutable, they can be efficiently merged, compacted, and queried concurrently without locking.

An SSTable file is logically divided into several components:

- Data Block Section: Contains the actual key-value entries, organized in sorted order. Each block holds a sequence of entries and is typically compressed for storage efficiency.

- Filter Block: Contains a Bloom filter to quickly test for key existence before reading data blocks(developing).

- Index Block: Holds offsets for each data block, mapping the last key of each block to its position in the file. This allows efficient binary search without scanning the entire file.

- Footer: Contains fixed-size metadata that points to the index and filter blocks, enabling random access to these components.

## Immutable Flow
The immutability of SSTables provides strong consistency and concurrency benefits.
Reads never need to lock SSTables since their content never changes. Writes always go to newer MemTables or newly created SSTables, avoiding write contention.

Over time, multiple SSTables accumulate, forming a hierarchy of levels (Level 0, Level 1, etc.) as part of the LSM-Tree (Log-Structured Merge Tree) design.
Background compaction processes periodically merge smaller SSTables into larger ones, discarding obsolete versions and maintaining lookup efficiency.

This separation of write (in-memory) and read (on-disk) paths allows the system to achieve high write throughput and predictable read latency.


## SST Read Path
When querying a key:

1. The system first checks the Bloom filter in the filter block — if the filter indicates “not present,” the read can be skipped immediately.

2. If possibly present, the index block is searched via binary search to locate the corresponding data block.

3. The data block is then read and optionally decompressed; a binary search inside this block locates the key-value pair.

This layered lookup minimizes disk reads, achieving efficient point and range queries even across many SSTables.

## SST Data Format
[Format](sst_dataformat.png)

The SST (Sorted String Table) file is a persistent, immutable structure that stores sorted key-value pairs on disk.
The format is designed for efficient range scans, fast point lookups, and compact metadata indexing.
The structure is divided into four primary sections: Block Section, Meta Section, Bloom Section, and Extra Information.

### Block Section

The Block Section contains the actual data blocks of the SST file.
Each data block stores multiple sorted key-value pairs (entries), each encoded with length prefixes and optional transaction IDs.

Structure:

```
| Block_0 | hash_0 | Block_1 | hash_1 | ... | Block_n | hash_n |
```

Block_i: Sequential data block storing entries in sorted order.

hash_i: Optional integrity checksum for verifying the block content.

Blocks are the basic unit of read and write operations on disk.
Each block is referenced in the Meta Section for indexing and efficient lookup.

### Meta Section

The Meta Section provides metadata about all blocks in the SST file.
It acts as an index that allows fast binary search and random access to specific data blocks.

Structure:

```
| Num | Meta_0 | Meta_1 | ... | Meta_n | Hash |
```

Num: Total number of data blocks in the SST.

Meta_i: Metadata entry describing the range and position of each block.

Hash: Optional checksum for metadata integrity.

Meta Entry (Meta_i) Format

Each Meta_i entry describes the location and key range of its corresponding data block.

```
| 1st_key_len(4B) | 1st_key | last_key_len(4B) | last_key | starting_offset_of_corresponding_data_block(8B) | size_of_data_block(8B) |
```

1st_key_len / 1st_key: Length and value of the first key in the block.

last_key_len / last_key: Length and value of the last key in the block.

starting_offset_of_corresponding_data_block: File offset where the block begins.

size_of_data_block: Total size (in bytes) of the block.

This metadata enables efficient key-range lookups without scanning all blocks.

### Bloom Section (Developing)

The Bloom Section is an optional, developing feature.
It will store Bloom filters that help quickly determine whether a key may exist in a block before performing a disk read.

Purpose:

Reduce unnecessary block reads.

Optimize point queries by probabilistically filtering out non-existent keys.

### Extra Information Section

The Extra Information Section contains global metadata and offset references to other sections in the file.
It is typically located at the end of the SST file.

Structure:


```
| Total block entries(8B) | Meta Section Offset(8B) | Bloom Section Offset(8B) | Min Tranc_ID(8B) | Max Tranc_ID(8B) |
```


Total block entries: Total number of key-value pairs across all blocks.

Meta Section Offset: Byte offset to the beginning of the Meta Section.

Bloom Section Offset: Byte offset to the Bloom Section (if enabled).

Min Tranc_ID / Max Tranc_ID: Range of transaction IDs for versioning or snapshot isolation.
