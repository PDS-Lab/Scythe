cmake_minimum_required(VERSION 3.10)
# 项目名称
project(Hot-T)

# 设置编译的C++标准
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (NOT CMAKE_BUILD_TYPE) 
  set(CMAKE_BUILD_TYPE "Debug")
endif()

message(STATUS "Build Type : ${CMAKE_BUILD_TYPE}")

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -g -DNDEBUG -fPIE")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIE")
endif()


include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include_directories("src" "third_party" "benchmark")
FILE(GLOB_RECURSE  SRC src/*.cc)

add_library(base ${SRC})
target_link_libraries(base zmq ibverbs boost_coroutine)

add_subdirectory(test)
add_subdirectory(benchmark)