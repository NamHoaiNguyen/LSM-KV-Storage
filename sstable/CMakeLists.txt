add_library(sstable
  block_builder.cc
  block_builder.h
  block_index.cc
  block_index.h
  block_reader.cc
  block_reader.h
  # table_builder.cc
  # table_builder.h
  table.cc
  table.h
)

target_include_directories(sstable PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(sstable PUBLIC io mvcc)