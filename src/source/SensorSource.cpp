#include "source/SensorSource.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <stdexcept>

namespace iot_stream {

/*
 * Kịch bản sử dụng điển hình:
 *   SensorSource source({"S1", "S2", "S3"}, std::chrono::milliseconds(100), std::chrono::seconds(10));
 *   source.subscribe(&mySubscriber);
 *   source.start();
 *   // main thread tiếp tục làm việc khác — không bị block
 *   // Sau ~10 giây: producerThread tự kết thúc, gọi onComplete cho tất cả subscriber
 *   source.stop(); // hoặc đợi join tự nhiên
 *
 * Phân tích tải:
 *   3 sensors × 10s / 0.1s = ~300 SensorReading được phát ra tổng cộng
 *   Mỗi SensorReading bao gồm: sensorId, temperature [20-40°C], humidity [30-90%], severity, timestamp
 */

SensorSource::SensorSource(std::vector<std::string> sensorIds,
                           std::chrono::milliseconds intervalMs,
                           std::chrono::seconds duration)
    : sensorIds_(std::move(sensorIds)),
      intervalMs_(intervalMs),
      duration_(duration),
      rng_(std::random_device{}()) {}

SensorSource::~SensorSource() {
    stop();
}

void SensorSource::subscribe(Subscriber<SensorReading>* subscriber) {
    if (!subscriber) return;
    std::lock_guard<std::mutex> lock(mtx_);
    subscribers_.push_back(subscriber);
}

void SensorSource::start() {
    if (running_.exchange(true)) {
        return; // Đã đang chạy, bỏ qua
    }

    // Spawn luồng producer bất đồng bộ — không chặn main thread
    producerThread_ = std::thread([this]() { producerLoop(); });
}

void SensorSource::stop() {
    running_ = false;
    if (producerThread_.joinable()) {
        producerThread_.join();
    }
}

void SensorSource::producerLoop() {
    // Phân phối xác suất Severity: 80% INFO, 15% WARN, 4% ERROR, 1% FATAL
    // Bảng phân bố phần trăm cộng dồn: [0,80) → INFO, [80,95) → WARN, [95,99) → ERROR, [99,100) → FATAL
    std::uniform_real_distribution<double> temp_dist(20.0, 40.0);   // Nhiệt độ: 20–40°C
    std::uniform_real_distribution<double> hum_dist(30.0, 90.0);    // Độ ẩm: 30–90%
    std::uniform_int_distribution<int> sev_dist(0, 99);             // 0–99 để tính xác suất
    std::uniform_int_distribution<int> sensor_idx(
        0, static_cast<int>(sensorIds_.size()) - 1);                // Chọn ngẫu nhiên cảm biến

    auto start_time = std::chrono::steady_clock::now();

    while (running_) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

        // Thoát khi đã hết thời lượng (duration > 0 mới áp dụng giới hạn)
        if (duration_.count() > 0 && elapsed >= duration_) {
            break;
        }

        // Phát một SensorReading cho MỖI sensor trong chu kỳ hiện tại
        for (const auto& id : sensorIds_) {
            if (!running_) break;

            // Sinh dữ liệu ngẫu nhiên cho SensorReading
            double temp = temp_dist(rng_);
            double hum  = hum_dist(rng_);

            // Xác định Severity theo phân phối xác suất IoT
            Severity sev = randomSeverity();

            SensorReading reading(
                id,
                temp,
                hum,
                sev,
                std::chrono::system_clock::now()
            );

            // Gửi dữ liệu tới tất cả subscribers (Push-based model)
            // Snapshot danh sách subscribers trước khi dispatch để tránh deadlock
            std::vector<Subscriber<SensorReading>*> targets;
            {
                std::lock_guard<std::mutex> lock(mtx_);
                targets = subscribers_;
            }

            for (auto* sub : targets) {
                if (sub) {
                    try {
                        sub->onNext(reading);
                    } catch (const std::exception& ex) {
                        try {
                            sub->onError(ex);
                        } catch (...) {}
                    }
                }
            }
        }

        // Chờ đúng chu kỳ intervalMs trước lần phát tiếp theo
        std::this_thread::sleep_for(intervalMs_);
    }

    // Phát tín hiệu hoàn tất (onComplete) tới tất cả subscriber
    // Gọi bên ngoài lock để tránh deadlock với subscriber gọi subscribe() đồng thời
    std::vector<Subscriber<SensorReading>*> targets;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        targets = subscribers_;
    }

    for (auto* sub : targets) {
        if (sub) {
            try {
                sub->onComplete();
            } catch (...) {}
        }
    }

    running_ = false;
}

Severity SensorSource::randomSeverity() {
    // Phân bố xác suất thực tế trong hệ thống IoT sản xuất:
    // - 80% INFO:  đo đạc bình thường, không có bất thường
    // - 15% WARN:  nhiệt độ hoặc độ ẩm vượt ngưỡng cảnh báo nhẹ
    //  - 4% ERROR: lỗi nghiêm trọng, cần can thiệp
    //  - 1% FATAL: sự cố nguy hiểm, cần dừng khẩn cấp
    std::uniform_int_distribution<int> dist(0, 99);
    int roll = dist(rng_);

    if (roll < 80)       return Severity::INFO;
    else if (roll < 95)  return Severity::WARN;
    else if (roll < 99)  return Severity::ERROR;
    else                 return Severity::FATAL;
}

} // namespace iot_stream

