cmake_minimum_required(VERSION 3.15)
project(iot_stream_benchmark LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Mặc định build Release (benchmark phải tối ưu)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# Tạo executable run_benchmarks
add_executable(run_benchmarks benchmark.cpp)

target_include_directories(run_benchmarks PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(run_benchmarks PRIVATE iot_stream Threads::Threads)

# Cấu hình thư mục xuất ra để đặt trực tiếp vào build/Release/
set_target_properties(run_benchmarks PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    RUNTIME_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}/Release"
    RUNTIME_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}/Debug"
)

# Tạo target phụ benchmark phụ thuộc vào run_benchmarks
add_custom_target(benchmark DEPENDS run_benchmarks)
