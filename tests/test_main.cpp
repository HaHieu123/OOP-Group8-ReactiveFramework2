/**
 * @file test_main.cpp
 * @brief Entry point duy nhất cho bộ test tổng hợp.
 * Gọi RUN_ALL_TESTS() — hàm này duyệt toàn bộ registry được đăng ký
 * qua macro TEST() trong các file test_*.cpp, chạy từng test, in kết quả.
 */

#include "test_framework.h"

int main() {
    return test_fw::RUN_ALL_TESTS();
}
