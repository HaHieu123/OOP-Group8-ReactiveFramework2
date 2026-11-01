#pragma once

#include "sinks/IDataSink.h"
#include "source/Sensor.h"

#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>

namespace iot_stream {

/**
 * @brief Sink ghi dữ liệu luồng ra tệp tin (File CSV / Log persistence).
 * 
 * Đặc điểm:
 * - Thread-safe: Mọi thao tác ghi dữ liệu xuống file đều được khóa bằng std::mutex.
 * - Mở file ở chế độ Append (`std::ios::out | std::ios::app`) giúp lưu trữ liên tục
 *   mà không làm mất dữ liệu đã ghi trước đó.
 * - Tự động ghi dòng tiêu đề CSV ("timestamp,sensor_id,temperature,humidity,severity")
 *   nếu tệp tin mới tạo hoặc đang rỗng và writeHeader = true.
 * - Destructor tự động flush và đóng file an toàn (RAII).
 * 
 * @tparam T Kiểu dữ liệu nhận từ luồng (Hỗ trợ SensorReading hoặc generic type).
 */
template <typename T>
class FileSink : public IDataSink<T> {
public:
    /**
     * @brief Khởi tạo FileSink và mở tệp tin.
     * @param filepath Đường dẫn tới tệp đích (ví dụ: "dashboard/data.csv").
     * @param writeHeader Có ghi dòng tiêu đề CSV nếu file mới hay không (mặc định = true).
     */
    explicit FileSink(const std::string& filepath, bool writeHeader = true)
        : filepath_(filepath) {
        std::lock_guard<std::mutex> lock(mtx_);

        // Kiểm tra xem file đã tồn tại và có dữ liệu hay chưa
        bool file_is_empty = true;
        {
            std::ifstream check_file(filepath_, std::ios::ate | std::ios::binary);
            if (check_file.is_open() && check_file.tellg() > 0) {
                file_is_empty = false;
            }
        }

        // Mở file ở chế độ append
        file_.open(filepath_, std::ios::out | std::ios::app);

        // Ghi tiêu đề CSV nếu file đang rỗng và writeHeader = true
        if (file_.is_open() && file_is_empty && writeHeader) {
            if constexpr (std::is_same_v<T, SensorReading>) {
                file_ << "timestamp,sensor_id,temperature,humidity,severity\n";
            } else {
                file_ << "timestamp,value\n";
            }
            file_.flush();
        }
    }

    ~FileSink() override {
        close();
    }

    /**
     * @brief Ghi một phần tử dữ liệu xuống file CSV.
     */
    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!file_.is_open()) return;

        if constexpr (std::is_same_v<T, SensorReading>) {
            file_ << to_csv_row(value) << "\n";
        } else {
            auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
            file_ << now_ms << "," << value << "\n";
        }
        file_.flush();
    }

    /**
     * @brief Ghi nhận ngoại lệ và flush dữ liệu xuống đĩa.
     */
    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (file_.is_open()) {
            file_ << "# ERROR: " << ex.what() << "\n";
            file_.flush();
        }
    }

    /**
     * @brief Nhận thông báo hoàn tất và flush dữ liệu.
     */
    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (file_.is_open()) {
            file_.flush();
        }
    }

    /**
     * @brief Đẩy toàn bộ bộ đệm file ra ổ đĩa vật lý.
     */
    void flush() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (file_.is_open()) {
            file_.flush();
        }
    }

    /**
     * @brief Đóng tệp tin an toàn.
     */
    void close() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (file_.is_open()) {
            file_.flush();
            file_.close();
        }
    }

    /**
     * @brief Kiểm tra tệp tin có đang mở hợp lệ hay không.
     */
    bool is_open() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return file_.is_open();
    }

private:
    std::string filepath_;
    std::ofstream file_;
    mutable std::mutex mtx_;
};

/**
 * @brief Factory helper tạo FileSink dạng std::shared_ptr.
 */
template <typename T>
inline std::shared_ptr<FileSink<T>> makeFileSink(const std::string& filepath, bool writeHeader = true) {
    return std::make_shared<FileSink<T>>(filepath, writeHeader);
}

} // namespace iot_stream

