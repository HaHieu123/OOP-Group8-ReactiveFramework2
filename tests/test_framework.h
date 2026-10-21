/**
 * @file test_framework.h
 * @brief Framework kiểm thử đơn giản, không phụ thuộc thư viện ngoài.
 *
 * Cung cấp:
 *  - Macro ASSERT_EQ, ASSERT_NE, ASSERT_TRUE, ASSERT_FALSE, ASSERT_NEAR, ASSERT_THROW
 *  - Macro TEST(SuiteName, TestName) { ... }  — đăng ký test tự động vào registry toàn cục
 *  - test_fw::RUN_ALL_TESTS()  — chạy tất cả test, in kết quả, trả về exit code
 *
 * Cách sử dụng:
 *   #include "test_framework.h"
 *   TEST(MyModule, BasicAdd) {
 *       ASSERT_EQ(1 + 1, 2);
 *   }
 *   // Trong test_main.cpp:
 *   int main() { return test_fw::RUN_ALL_TESTS(); }
 */

#pragma once

#include <cmath>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace test_fw {

// ─────────────────────────────────────────────────────────────────────────────
// Kiểu ngoại lệ nội bộ dùng khi một assertion thất bại
// ─────────────────────────────────────────────────────────────────────────────
struct AssertionFailure {
    std::string message;
    explicit AssertionFailure(std::string msg) : message(std::move(msg)) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// Registry toàn cục lưu danh sách các test case
// ─────────────────────────────────────────────────────────────────────────────
struct TestCase {
    std::string suite_name;
    std::string test_name;
    std::function<void()> body;
};

inline std::vector<TestCase>& test_registry() {
    static std::vector<TestCase> registry;
    return registry;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper đăng ký test (được gọi bởi macro TEST)
// ─────────────────────────────────────────────────────────────────────────────
struct TestRegistrar {
    TestRegistrar(const char* suite, const char* test, std::function<void()> body) {
        test_registry().push_back({suite, test, std::move(body)});
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// SFINAE helper: to_debug_str — chuyển giá trị sang string để in thông báo lỗi.
// Nếu T có operator<<, dùng nó; nếu không, in "<non-printable>".
// PHẢI được khai báo TRƯỚC các assert_*_impl.
// ─────────────────────────────────────────────────────────────────────────────
template <typename T, typename = void>
struct has_ostream_op : std::false_type {};

template <typename T>
struct has_ostream_op<T,
    std::void_t<decltype(std::declval<std::ostream&>() << std::declval<const T&>())>>
    : std::true_type {};

template <typename T>
inline std::string to_debug_str(const T& val) {
    if constexpr (has_ostream_op<T>::value) {
        std::ostringstream oss;
        oss << val;
        return oss.str();
    } else {
        (void)val;
        return "<non-printable>";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Hàm triển khai assertion — tạo thông điệp lỗi và ném AssertionFailure
// ─────────────────────────────────────────────────────────────────────────────
template <typename A, typename B>
inline void assert_eq_impl(const A& a, const B& b,
                           const char* expr_a, const char* expr_b,
                           const char* file, int line) {
    if (!(a == b)) {
        std::ostringstream oss;
        oss << file << ":" << line
            << " -- ASSERT_EQ(" << expr_a << ", " << expr_b << ")"
            << "  left=" << to_debug_str(a) << "  right=" << to_debug_str(b);
        throw AssertionFailure(oss.str());
    }
}

template <typename A, typename B>
inline void assert_ne_impl(const A& a, const B& b,
                           const char* expr_a, const char* expr_b,
                           const char* file, int line) {
    if (!(a != b)) {
        std::ostringstream oss;
        oss << file << ":" << line
            << " -- ASSERT_NE(" << expr_a << ", " << expr_b << ")"
            << "  both=" << to_debug_str(a);
        throw AssertionFailure(oss.str());
    }
}

inline void assert_true_impl(bool val, const char* expr,
                             const char* file, int line) {
    if (!val) {
        std::ostringstream oss;
        oss << file << ":" << line
            << " -- ASSERT_TRUE(" << expr << ") is false";
        throw AssertionFailure(oss.str());
    }
}

inline void assert_false_impl(bool val, const char* expr,
                              const char* file, int line) {
    if (val) {
        std::ostringstream oss;
        oss << file << ":" << line
            << " -- ASSERT_FALSE(" << expr << ") is true";
        throw AssertionFailure(oss.str());
    }
}

inline void assert_near_impl(double a, double b, double eps,
                             const char* expr_a, const char* expr_b,
                             const char* file, int line) {
    if (std::abs(a - b) > eps) {
        std::ostringstream oss;
        oss << file << ":" << line
            << " -- ASSERT_NEAR(" << expr_a << ", " << expr_b << ")"
            << "  |" << a << " - " << b << "| = " << std::abs(a - b)
            << "  > eps=" << eps;
        throw AssertionFailure(oss.str());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Chạy tất cả test và in tổng kết — trả về 0 nếu tất cả PASS, 1 nếu có FAIL
// ─────────────────────────────────────────────────────────────────────────────
inline int RUN_ALL_TESTS() {
    auto& tests = test_registry();
    int passed = 0;
    int failed = 0;

    std::cout << "\n+======================================================+\n";
    std::cout <<   "|       IoT Stream Framework -- Test Runner            |\n";
    std::cout <<   "+======================================================+\n";
    std::cout << "Running " << tests.size() << " tests...\n\n";

    std::string current_suite;
    for (auto& tc : tests) {
        // In header nhóm suite khi chuyển sang suite mới
        if (tc.suite_name != current_suite) {
            current_suite = tc.suite_name;
            std::cout << "  [Suite: " << current_suite << "]\n";
        }

        std::cout << "    >> " << tc.suite_name << "::" << tc.test_name << " ... ";
        std::cout.flush();

        try {
            tc.body();
            std::cout << "PASS\n";
            ++passed;
        } catch (const AssertionFailure& af) {
            std::cout << "FAIL\n";
            std::cout << "      x " << af.message << "\n";
            ++failed;
        } catch (const std::exception& ex) {
            std::cout << "FAIL (unexpected exception)\n";
            std::cout << "      x " << ex.what() << "\n";
            ++failed;
        } catch (...) {
            std::cout << "FAIL (unknown exception)\n";
            ++failed;
        }
    }

    int total = passed + failed;
    std::cout << "\n------------------------------------------------------\n";
    std::cout << "  Total : " << total  << "\n";
    std::cout << "  Passed: " << passed << "\n";
    std::cout << "  Failed: " << failed << "\n";
    std::cout << "------------------------------------------------------\n";
    std::cout << (failed == 0 ? "  >>> ALL TESTS PASSED <<<\n"
                              : "  !!! SOME TESTS FAILED !!!\n");
    std::cout << "------------------------------------------------------\n\n";

    return failed == 0 ? 0 : 1;
}

} // namespace test_fw

// ─────────────────────────────────────────────────────────────────────────────
// Public Macros
// ─────────────────────────────────────────────────────────────────────────────

/** So sánh bằng: ASSERT_EQ(a, b) */
#define ASSERT_EQ(a, b) \
    ::test_fw::assert_eq_impl((a), (b), #a, #b, __FILE__, __LINE__)

/** So sánh khác nhau: ASSERT_NE(a, b) */
#define ASSERT_NE(a, b) \
    ::test_fw::assert_ne_impl((a), (b), #a, #b, __FILE__, __LINE__)

/** Kiểm tra đúng: ASSERT_TRUE(expr) */
#define ASSERT_TRUE(expr) \
    ::test_fw::assert_true_impl(static_cast<bool>(expr), #expr, __FILE__, __LINE__)

/** Kiểm tra sai: ASSERT_FALSE(expr) */
#define ASSERT_FALSE(expr) \
    ::test_fw::assert_false_impl(static_cast<bool>(expr), #expr, __FILE__, __LINE__)

/**
 * Kiểm tra gần đúng với epsilon: ASSERT_NEAR(a, b, eps)
 * Ví dụ: ASSERT_NEAR(3.14159, M_PI, 1e-5)
 */
#define ASSERT_NEAR(a, b, eps) \
    ::test_fw::assert_near_impl(static_cast<double>(a), static_cast<double>(b), \
                                static_cast<double>(eps), #a, #b, __FILE__, __LINE__)

/**
 * Kiểm tra ngoại lệ: ASSERT_THROW(expr, ExceptionType)
 * Thất bại nếu expr KHÔNG ném ExceptionType.
 */
#define ASSERT_THROW(expr, ExType)                                              \
    do {                                                                        \
        bool _caught = false;                                                   \
        try { (expr); }                                                         \
        catch (const ExType&) { _caught = true; }                              \
        catch (...) {}                                                          \
        if (!_caught) {                                                         \
            std::ostringstream _oss;                                            \
            _oss << __FILE__ << ":" << __LINE__                                 \
                 << " -- ASSERT_THROW(" #expr ", " #ExType "): exception not thrown"; \
            throw ::test_fw::AssertionFailure(_oss.str());                      \
        }                                                                       \
    } while (0)

/**
 * Macro đăng ký một test case vào registry toàn cục.
 * Hỗ trợ cả 2 dạng:
 *   TEST(SuiteName, TestName) { ... }
 *   TEST(TestName) { ... }
 */
#define TEST_IMPL_1(Name)                                                       \
    static void _test_body_##Name();                                            \
    static ::test_fw::TestRegistrar _test_reg_##Name(                           \
        "Window", #Name, _test_body_##Name);                                    \
    static void _test_body_##Name()

#define TEST_IMPL_2(Suite, Name)                                                \
    static void _test_body_##Suite##_##Name();                                  \
    static ::test_fw::TestRegistrar _test_reg_##Suite##_##Name(                 \
        #Suite, #Name, _test_body_##Suite##_##Name);                            \
    static void _test_body_##Suite##_##Name()

#define TEST_EXPAND(x) x
#define GET_MACRO_TEST(_1, _2, NAME, ...) NAME
#define TEST(...) TEST_EXPAND(GET_MACRO_TEST(__VA_ARGS__, TEST_IMPL_2, TEST_IMPL_1)(__VA_ARGS__))

