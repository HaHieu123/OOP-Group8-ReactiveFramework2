#pragma once

#include "core/Publisher.h"
#include "core/Subscriber.h"
#include "source/Sensor.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace iot_stream {

/**
 * @brief Nguồn phát dữ liệu cảm biến IoT (Simulated Sensor Source / Producer).
 * 
 * Kịch bản điển hình:
 *   SensorSource({S1, S2, S3}, 100ms, 10s)
 *   → mỗi chu kỳ 100ms, mỗi sensor phát 1 SensorReading
 *   → 3 sensors × 10s / 0.1s = ~300 readings tổng cộng
 * 
 * Mô hình Push-based, bất đồng bộ:
 * - Luồng phát dữ liệu (producerThread_) chạy độc lập, không chặn main thread.
 * - Gọi start() để khởi tạo luồng sản xuất.
 * - Gọi stop() hoặc đợi hết duration để kết thúc an toàn.
 */
class SensorSource : public Publisher<SensorReading> {
public:
    /**
     * @brief Khởi tạo SensorSource với danh sách sensor, chu kỳ và thời lượng.
     * @param sensorIds Danh sách ID cảm biến (ví dụ: {"S1", "S2", "S3"}).
     * @param intervalMs Chu kỳ phát dữ liệu (ví dụ: 100ms → 10Hz).
     * @param duration Thời lượng mô phỏng (0 = chạy vô hạn cho tới khi stop() được gọi).
     */
    SensorSource(std::vector<std::string> sensorIds,
                 std::chrono::milliseconds intervalMs,
                 std::chrono::seconds duration);

    /**
     * @brief Destructor: Dừng luồng phát và giải phóng tài nguyên an toàn (RAII).
     */
    ~SensorSource() override;

    /**
     * @brief Đăng ký một Subscriber để nhận SensorReading từ luồng phát.
     * Phải được gọi TRƯỚC khi start().
     * @param subscriber Con trỏ Subscriber<SensorReading>.
     */
    void subscribe(Subscriber<SensorReading>* subscriber) override;

    /**
     * @brief Khởi động luồng phát dữ liệu bất đồng bộ (non-blocking).
     * Luồng producer bắt đầu phát SensorReading ngay sau khi start() trả về.
     * Gọi stop() hoặc chờ duration hết hạn để kết thúc.
     */
    void start();

    /**
     * @brief Dừng luồng phát và đợi thread kết thúc an toàn (blocking join).
     */
    void stop();

    /**
     * @brief Kiểm tra luồng phát đang hoạt động hay không.
     */
    bool isRunning() const { return running_.load(); }

private:
    /**
     * @brief Hàm nội bộ chạy bên trong producerThread_.
     * Thực thi vòng lặp phát dữ liệu, tính Severity theo xác suất, và gọi subscriber->onNext.
     */
    void producerLoop();

    /**
     * @brief Sinh ngẫu nhiên mức Severity theo phân phối xác suất IoT thực tế:
     *   80% INFO, 15% WARN, 4% ERROR, 1% FATAL.
     */
    Severity randomSeverity();

    std::vector<std::string> sensorIds_;
    std::chrono::milliseconds intervalMs_;
    std::chrono::seconds duration_;
    std::atomic<bool> running_{false};
    std::thread producerThread_;
    std::vector<Subscriber<SensorReading>*> subscribers_;
    std::mutex mtx_;
    std::mt19937 rng_;
};

} // namespace iot_stream

