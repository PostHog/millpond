cmake_minimum_required(VERSION 3.16)
project(arrow_consumer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -O2 -DNDEBUG)

# Arrow C++ (record batch reader + C data interface bridge)
find_package(Arrow REQUIRED)

# librdkafka ships a pkg-config file; use it for the C API
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka)

# DuckDB shared library. Headers + libduckdb.so are installed into the
# Dockerfile.consumer image by the infra agent (/usr/local/include + /usr/local/lib).
find_library(DUCKDB_LIB duckdb REQUIRED)
find_path(DUCKDB_INCLUDE_DIR duckdb.h REQUIRED)

add_executable(arrow-consumer main.cpp)

target_include_directories(arrow-consumer PRIVATE ${DUCKDB_INCLUDE_DIR})

target_link_libraries(arrow-consumer
    PRIVATE
        Arrow::arrow_shared
        PkgConfig::RDKAFKA
        ${DUCKDB_LIB}
)
