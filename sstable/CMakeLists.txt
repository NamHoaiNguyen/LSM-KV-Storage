add_library(sstable
  block_builder.cc
  block_builder.h
  block_index.cc
  block_index.h
  block_reader_cache.cc
  block_reader_cache.h
  block_reader_iterator.cc
  block_reader_iterator.h
  block_reader.cc
  block_reader.h
  lru_table_item.cc
  lru_table_item.h
  table_builder.cc
  table_builder.h
  table_reader_cache.cc
  table_reader_cache.h
  table_reader_iterator.cc
  table_reader_iterator.h
  table_reader.cc
  table_reader.h
)

target_include_directories(sstable PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(sstable PUBLIC io mvcc)
target_link_libraries(sstable PRIVATE RapidJSON)
