cmake_minimum_required(VERSION 3.16)
project(jarvisql LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_library(jarvisql_core
  src/engine.cpp
  src/sql_parser.cpp
  src/protocol.cpp
  src/persistence.cpp
)

target_include_directories(jarvisql_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(jarvisql_core PRIVATE -Wall -Wextra -Wpedantic)

add_library(flexql_client
  src/client_api.cpp
  src/protocol.cpp
)

target_include_directories(flexql_client
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(flexql_client PRIVATE -Wall -Wextra -Wpedantic)

add_executable(jarvisql_server src/server.cpp)
target_link_libraries(jarvisql_server PRIVATE jarvisql_core pthread)

add_executable(jarvisql_repl src/repl.cpp)
target_link_libraries(jarvisql_repl PRIVATE flexql_client)

add_executable(jarvisql_bench src/bench.cpp)
target_link_libraries(jarvisql_bench PRIVATE flexql_client)

add_executable(benchmark_flexql tests/benchmark_flexql.cpp)
target_link_libraries(benchmark_flexql PRIVATE flexql_client)

enable_testing()
add_executable(jarvisql_tests
  tests/test_engine.cpp
)
target_link_libraries(jarvisql_tests PRIVATE jarvisql_core pthread)
add_test(NAME jarvisql_tests COMMAND jarvisql_tests)
