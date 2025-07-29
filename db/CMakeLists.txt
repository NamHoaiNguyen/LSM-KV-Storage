add_library(db
  base_memtable.h
  db.cc
  db.h
  lsm.cc
  lsm.h
  memtable.cc
  memtable.h
  skiplist_node.cc
  skiplist_node.h
  skiplist.cc
  skiplist.h
)

target_include_directories(db PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(db PUBLIC common)