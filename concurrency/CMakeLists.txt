add_library(concurrency
  transaction_manager.cc
  transaction_manager.h
  transaction.cc
  transaction.h
)

target_include_directories(concurrency PUBLIC ${CMAKE_SOURCE_DIR})