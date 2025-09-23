add_library(db
  base_memtable.h
  compact.cc
  compact.h
  config.cc
  config.h
  db_impl.cc
  db_impl.h
  memtable_iterator.cc
  memtable_iterator.h
  memtable.cc
  memtable.h
  merge_iterator.cc
  merge_iterator.h
  skiplist_iterator.cc
  skiplist_iterator.h
  skiplist_node.cc
  skiplist_node.h
  skiplist.cc
  skiplist.h
  status.h
  version_edit.h
  version_edit.cc
  version_manager.cc
  version_manager.h
  version.cc
  version.h
)

target_include_directories(db PUBLIC ${CMAKE_SOURCE_DIR})
# target_include_directories(db PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(db PUBLIC io mvcc sstable)