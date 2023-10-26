function(add_test TEST_FILE)
  get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
  add_executable(${TEST_NAME} ${TEST_FILE})
  target_link_libraries(${TEST_NAME} 
    base
    gtest_main
  )
  message(STATUS "[ADD TEST] : ${TEST_NAME}")
  # gtest_discover_tests(${TEST_NAME})
endfunction()

FILE(GLOB_RECURSE  test_files *_test.cc)
# message(STATUS "${test_files}")

foreach(test_file ${test_files})
  add_test(${test_file})
endforeach(test_file)

add_executable(bench_runner bench_runner.cc)

target_link_libraries(bench_runner micro_db micro_txn smallbank_db smallbank_txn tpcc_db tpcc_txn)

message(STATUS "[ADD BENCH_RUNNER]")

add_subdirectory(micro)
add_subdirectory(tpcc)
add_subdirectory(smallbank)
