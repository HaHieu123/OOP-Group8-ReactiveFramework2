# iot_stream_framework

> Reactive Object-Oriented Stream Processing Pipeline Framework gọn nhẹ, hiệu năng cao bằng C++17 cho IoT Edge.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/username/iot_stream_framework/actions)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![CMake](https://img.shields.io/badge/CMake-3.16+-orange.svg)](https://cmake.org)
[![Python](https://img.shields.io/badge/Python-3.9+-yellow.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Chi tiết thiết kế và nguyên lý hoạt động: xem tại [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## ⚙️ Yêu cầu hệ thống
- **C++ Compiler**: Hỗ trợ chuẩn **C++17** (GCC 8+, Clang 7+, MSVC 2019+).
- **Build System**: **CMake 3.16+**.
- **Dashboard**: **Python 3.9+** (cho module trực quan hóa).
- **Không thư viện ngoài cho C++**: 100% thuần C++ Standard Library & `Threads::Threads`.

---

## 🚀 Build & Run (3 lệnh)

```bash
# 1. Cấu hình build
cmake -B build -DCMAKE_BUILD_TYPE=Release

# 2. Biên dịch framework & app
cmake --build build -j

# 3. Khởi chạy ứng dụng streaming
./build/iot_app
```

> **Gợi ý**: Chạy toàn bộ luồng C++ kết hợp Dashboard Python bằng script:
> ```bash
> chmod +x scripts/run_demo.sh && ./scripts/run_demo.sh
> ```

---

## 💡 Ví dụ Fluent API

```cpp
#include "source/SensorSource.h"
#include "operators/Operators.h"
#include "operators/Window.h"
#include "sinks/ConsoleSink.h"

// Xây dựng Pipeline xử lý dữ liệu cảm biến thời gian thực:
source.filter([](const SensorData& d) { return d.severity >= Severity::WARN; })
      .map([](const SensorData& d) { return SummaryRecord{d.id, d.temp, d.humidity}; })
      .window(TumblingWindow(std::chrono::seconds(2)))
      .to(std::make_shared<ConsoleSink>());
```

---

## 📁 Cấu trúc thư mục tóm tắt

```text
iot_stream_framework/
├── CMakeLists.txt              # Cấu hình build chính (CMake 3.16+, C++17)
├── .clang-format               # Chuẩn format mã nguồn (LLVM, 4 spaces, 100 cols)
├── .gitignore                  # Bỏ qua file build, artifacts & runtime data.csv
├── config/pipeline.yaml        # Cấu hình nguồn phát, backpressure, windowing, sinks
├── docs/ARCHITECTURE.md        # Tài liệu kiến trúc, sơ đồ luồng, bảng ánh xạ Design Pattern
├── scripts/run_demo.sh         # Script tự động build & chạy demo C++ kèm Dashboard
├── include/                    # Headers: core/, source/, operators/, backpressure/, sinks/
├── src/                        # Implementations & main.cpp
├── tests/                      # Unit tests & integration tests (Zero-external-test-runner)
├── benchmark/                  # Benchmark đo lường độ trễ và thông lượng
├── dashboard/                  # Dashboard trực quan hóa dữ liệu thời gian thực (Python)
└── uml/                        # Sơ đồ UML Class, Sequence & Flow diagrams
```
