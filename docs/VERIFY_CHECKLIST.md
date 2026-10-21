# ✅ VERIFY CHECKLIST — IoT Stream Framework

## A. Build
- [x] `cmake -B build` không lỗi
- [x] `cmake --build build --config Release` không lỗi
- [x] Có 3 file: build/Release/iot_app.exe, run_benchmarks.exe, test_all.exe

## B. Unit Tests
- [x] `cd build; ctest -C Release` → 100% pass (43/43 tests pass)
- [x] test_core: publisher emit 100 OK
- [x] test_operators: map/filter/flatMap OK
- [x] test_window: tumbling/sliding/count + Stats OK
- [x] test_sink: console/file/memory OK
- [x] test_backpressure: 4 strategy OK
- [x] test_integration: end-to-end OK

## C. App Demo
- [x] `.\build\Release\iot_app.exe` in [INIT], [RUN], [SUMMARY]
- [x] Sinh file `dashboard/data.csv` với header + >0 dòng dữ liệu
- [x] Console hiển thị tiếng Việt đúng (không lỗi font với UTF-8 CP_UTF8)
- [x] Summary có: total events, duration, alerts, avg latency
- [x] Ctrl+C dừng sạch (SIGINT handler an toàn)

## D. Benchmark
- [x] `.\build\Release\run_benchmarks.exe` in bảng throughput
- [x] Throughput > 100,000 events/sec (~6,800,000 events/sec)
- [x] Bảng 4 backpressure strategy có đủ cột (Strategy, Throughput, p50, p99, Dropped)

## E. Dashboard (Python)
- [ ] `pip install -r dashboard/requirements.txt` OK
- [ ] `python dashboard/dashboard.py --file data.csv` mở cửa sổ
- [ ] 3 subplot hiển thị: line temp, bar severity, rolling avg
- [ ] Cập nhật realtime mỗi 1 giây

## F. UML
- [ ] `plantuml uml/class_diagram.puml` sinh PNG
- [ ] `plantuml uml/sequence_diagram.puml` sinh PNG
- [ ] Mở flow_diagram.drawio bằng app.diagrams.net OK

## G. Config linh hoạt (demo hội đồng)
- [ ] Sửa config/pipeline.yaml: strategy BUFFER → DROP
- [ ] Chạy lại app → không cần sửa code, behavior đổi
- [ ] Summary hiển thị dropped > 0 khi dùng DROP

