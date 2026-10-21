# tests/CMakeLists.txt
# ─────────────────────────────────────────────────────────────────────────────
# Xây dựng một binary test_all duy nhất gồm:
#   - test_main.cpp        : entry point, gọi RUN_ALL_TESTS()
#   - test_framework.h     : custom test framework (header-only)
#   - test_core.cpp        : Core tests
#   - test_operators.cpp   : Operators tests
#   - test_window.cpp      : Window + Stats tests
#   - test_stats.cpp       : Stats sanity check tests
#   - test_sink.cpp        : Sink tests
#   - test_backpressure.cpp: Backpressure tests
#   - test_integration.cpp : Integration tests
# ─────────────────────────────────────────────────────────────────────────────

# Thu thập tất cả file test .cpp (bao gồm test_main.cpp)
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# Tạo binary test_all từ tất cả source files
add_executable(test_all ${TEST_SOURCES})

target_include_directories(test_all PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}   # để tìm test_framework.h
    ${PROJECT_SOURCE_DIR}/include # headers của framework
)

target_link_libraries(test_all PRIVATE iot_stream Threads::Threads)

# Đảm bảo C++17 và output directory vào build/Release cho binary test
set_target_properties(test_all PROPERTIES 
    CXX_STANDARD 17 
    CXX_STANDARD_REQUIRED ON
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/Release"
    RUNTIME_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}/Release"
    RUNTIME_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}/Debug"
)

# Đăng ký với CTest
add_test(NAME all_tests COMMAND test_all)
