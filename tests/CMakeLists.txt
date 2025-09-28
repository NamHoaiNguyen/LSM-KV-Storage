file(GLOB TEST_SOURCES *.cc)

foreach(test_file ${TEST_SOURCES})
    get_filename_component(test_name ${test_file} NAME_WE)

    add_executable(${test_name} ${test_file})

    # Include headers from all modules
    target_include_directories(${test_name} PRIVATE
        ${PROJECT_SOURCE_DIR}/common
        ${PROJECT_SOURCE_DIR}/io
        ${PROJECT_SOURCE_DIR}/sstable
        # ${PROJECT_SOURCE_DIR}/storage
        ${PROJECT_SOURCE_DIR}/db
    )

    target_link_libraries(${test_name}
        GTest::gtest_main
        # common
        io
        sstable
        # storage
        db
        RapidJSON
    )

    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
