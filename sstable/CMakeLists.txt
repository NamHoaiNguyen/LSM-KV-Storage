add_library(sstable
  # block_builder.cc
  # block_builder.h
  block_index.cc
  block_index.h
  block.cc
  block.h
  # table_builder.cc
  # table_builder.h
  sst.cc
  sst.h
)

target_include_directories(sstable PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(sstable PUBLIC concurrency io)