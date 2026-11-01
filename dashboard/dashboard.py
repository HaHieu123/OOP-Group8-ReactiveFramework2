#!/usr/bin/env python3
"""
dashboard.py — Dashboard trực quan hóa realtime đọc CSV do FileSink ghi ra.

Đặc điểm:
- Tail-style: poll file mỗi `--refresh` giây, chỉ đọc phần mới (byte offset),
  bỏ qua dòng cuối chưa hoàn chỉnh (chưa có '\n').
- Tự động nhận diện file CSV tại 'dashboard/data.csv' hoặc 'data.csv'.
- Tự động reset offset nếu file CSV bị xóa/tạo lại khi app khởi động lại.
- 3 subplot qua FuncAnimation:
    (1) Line: temperature theo thời gian, mỗi sensor một đường màu riêng
    (2) Bar : phân bố severity INFO/WARN/ERROR/FATAL
    (3) Line: rolling average temperature (window=10 điểm)
- Console: in events/sec + tổng số events mỗi giây.
- Hiển thị khung đồ thị và trạng thái chờ ngay khi mở cửa sổ.
- Ctrl+C dừng sạch sẽ.

Cách chạy:
    python dashboard.py
    hoặc:
    python dashboard.py --file data.csv --refresh 1
"""

import argparse
import io
import os
import sys
import time

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd


# ===========================================================================
# Cấu hình Matplotlib Backend tương tác an toàn
# ===========================================================================
def setup_matplotlib_backend():
    """Thử các backend đồ họa tương tác phổ biến, tránh crash nếu thiếu Tkinter."""
    interactive_backends = ['TkAgg', 'Qt5Agg', 'QtAgg', 'macosx', 'WXAgg']
    current = matplotlib.get_backend().lower()
    if 'agg' not in current or current in ['tkagg', 'qtagg', 'qt5agg']:
        return

    for backend in interactive_backends:
        try:
            matplotlib.use(backend)
            break
        except Exception:
            continue


# ===========================================================================
# Helper tìm file CSV linh hoạt theo thư mục thực thi
# ===========================================================================
def resolve_csv_path(input_path: str) -> str:
    """Tự động tìm kiếm vị trí file data.csv nếu đường dẫn ban đầu không tồn tại."""
    if os.path.exists(input_path):
        return input_path

    # Danh sách các ứng viên phổ biến
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        input_path,
        'data.csv',
        'dashboard/data.csv',
        os.path.join(script_dir, 'data.csv'),
        os.path.join(script_dir, '..', 'dashboard', 'data.csv'),
        os.path.join(script_dir, '..', 'data.csv')
    ]
    for c in candidates:
        if os.path.exists(c):
            return c

    # Nếu chưa tồn tại, trả về input_path (sẽ đợi app tạo ra)
    return input_path


# ===========================================================================
# Tail CSV: đọc phần mới của file kể từ byte offset trước đó
# ===========================================================================
class CSVTail:
    """Đọc file CSV theo kiểu `tail -f` — chỉ đọc các dòng mới xuất hiện."""

    def __init__(self, path: str):
        self.path = path
        self.offset = 0          # byte offset đã đọc xong
        self.columns = None      # danh sách tên cột

    def read_new(self) -> pd.DataFrame:
        actual_path = resolve_csv_path(self.path)
        self.path = actual_path

        if not os.path.exists(self.path):
            self.offset = 0
            self.columns = None
            return pd.DataFrame()

        try:
            file_size = os.path.getsize(self.path)
            # Nếu file bị xóa rồi tạo lại hoặc bị truncate nhỏ hơn offset cũ
            if file_size < self.offset:
                self.offset = 0
                self.columns = None

            with open(self.path, 'rb') as f:
                f.seek(self.offset)
                data = f.read()
        except OSError:
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        # Chỉ xử lý tới ký tự newline cuối cùng — bỏ dòng đang ghi dở
        last_nl = data.rfind(b'\n')
        if last_nl == -1:
            return pd.DataFrame()

        complete = data[:last_nl + 1]
        self.offset += len(complete)
        text = complete.decode('utf-8', errors='replace')

        # Xử lý header ở lần đọc đầu tiên
        if self.columns is None:
            lines = text.splitlines()
            if not lines:
                return pd.DataFrame()

            first_line = lines[0].strip()
            # Kiểm tra xem dòng đầu có phải tiêu đề CSV không
            if 'timestamp' in first_line.lower() or 'sensor' in first_line.lower():
                self.columns = [c.strip() for c in first_line.split(',')]
                rest = '\n'.join(lines[1:])
            else:
                # Không có tiêu đề, gán schema chuẩn của SensorReading
                parts = [c.strip() for c in first_line.split(',')]
                if len(parts) >= 5:
                    self.columns = ['timestamp', 'sensor_id', 'temperature', 'humidity', 'severity']
                elif len(parts) == 2:
                    self.columns = ['timestamp', 'value']
                else:
                    self.columns = [f'col_{i}' for i in range(len(parts))]
                rest = text

            if not rest.strip():
                return pd.DataFrame()
            buf = io.StringIO(rest)
        else:
            buf = io.StringIO(text)

        try:
            df = pd.read_csv(buf, header=None, names=self.columns)
        except Exception as exc:
            print(f"[dashboard] Cảnh báo parse CSV: {exc}", file=sys.stderr)
            return pd.DataFrame()
        return df


# ===========================================================================
# State tích lũy dùng chung giữa các khung hình (frames)
# ===========================================================================
class State:
    MAX_ROWS = 5000      # Giới hạn bộ nhớ: chỉ giữ 5000 mẫu dữ liệu gần nhất

    def __init__(self):
        self.df = pd.DataFrame()
        self.events_since_last = 0
        self.total_events = 0
        self.last_print = 0.0


# ===========================================================================
# Vẽ Subplot (1): Line Temperature theo thời gian (phân theo từng Sensor)
# ===========================================================================
def draw_temperature(ax, df: pd.DataFrame):
    ax.clear()
    ax.set_title("Nhiệt độ thời gian thực (°C) — Từng cảm biến", fontsize=10, fontweight='bold')
    ax.set_ylabel("Nhiệt độ (°C)")

    if df.empty or 'temperature' not in df.columns:
        ax.text(0.5, 0.5, "Đang chờ dữ liệu telemetry từ IoT app...",
                ha='center', va='center', transform=ax.transAxes, color='#757575', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.4)
        return

    work = df.copy()

    # Chuẩn hóa timestamp (hỗ trợ cả epoch ms, s và chuỗi ISO)
    if 'timestamp' in work.columns:
        if pd.api.types.is_numeric_dtype(work['timestamp']) or work['timestamp'].astype(str).str.isdigit().all():
            ts_num = pd.to_numeric(work['timestamp'], errors='coerce')
            if (ts_num > 1e11).any():
                work['timestamp'] = pd.to_datetime(ts_num, unit='ms', errors='coerce')
            elif (ts_num > 1e8).any():
                work['timestamp'] = pd.to_datetime(ts_num, unit='s', errors='coerce')
            else:
                work['timestamp'] = pd.to_datetime(ts_num, errors='coerce')
        else:
            work['timestamp'] = pd.to_datetime(work['timestamp'], errors='coerce')

    work['temperature'] = pd.to_numeric(work['temperature'], errors='coerce')
    work = work.dropna(subset=['temperature'])
    if work.empty:
        ax.text(0.5, 0.5, "Chưa có dữ liệu nhiệt độ hợp lệ...",
                ha='center', va='center', transform=ax.transAxes, color='#757575')
        ax.grid(True, linestyle='--', alpha=0.4)
        return

    use_time = ('timestamp' in work.columns and work['timestamp'].notna().any())
    if use_time:
        work = work.sort_values('timestamp')

    palette = ['#1E88E5', '#D81B60', '#004D40', '#FB8C00', '#8E24AA']
    if 'sensor_id' in work.columns and work['sensor_id'].notna().any():
        for idx, (sensor, grp) in enumerate(work.groupby('sensor_id')):
            color = palette[idx % len(palette)]
            x_vals = grp['timestamp'] if use_time else grp.index
            ax.plot(x_vals, grp['temperature'],
                    label=f"Sensor {sensor}", linewidth=1.5, marker='o', markersize=3, color=color)
        ax.legend(loc='upper right', fontsize=8, framealpha=0.85)
    else:
        x_vals = work['timestamp'] if use_time else work.index
        ax.plot(x_vals, work['temperature'], color='#1E88E5', linewidth=1.5, label='Nhiệt độ')
        ax.legend(loc='upper right', fontsize=8)

    ax.tick_params(axis='x', labelrotation=20, labelsize=8)
    ax.grid(True, linestyle='--', alpha=0.5)


# ===========================================================================
# Vẽ Subplot (2): Bar Chart phân bố mức độ cảnh báo Severity
# ===========================================================================
def draw_severity(ax, df: pd.DataFrame):
    ax.clear()
    ax.set_title("Phân bố mức cảnh báo Severity (DEBUG / INFO / WARN / ERROR / FATAL)", fontsize=10, fontweight='bold')
    ax.set_ylabel("Số sự kiện")

    if df.empty or 'severity' not in df.columns:
        ax.text(0.5, 0.5, "Đang chờ dữ liệu severity...",
                ha='center', va='center', transform=ax.transAxes, color='#757575', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.4)
        return

    counts = df['severity'].astype(str).str.strip().value_counts()
    if counts.empty:
        ax.text(0.5, 0.5, "Đang chờ dữ liệu severity...",
                ha='center', va='center', transform=ax.transAxes, color='#757575')
        ax.grid(True, linestyle='--', alpha=0.4)
        return

    order = [s for s in ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL') if s in counts.index]
    order += [s for s in counts.index if s not in order]
    colors = {
        'DEBUG': '#90CAF9',
        'INFO': '#43A047',
        'WARN': '#FB8C00',
        'ERROR': '#E53935',
        'FATAL': '#880E4F'
    }

    bars = ax.bar(order, [counts[s] for s in order],
                  color=[colors.get(s, '#78909C') for s in order], width=0.5)

    # Hiển thị số lượng sự kiện phía trên mỗi cột
    for b in bars:
        h = b.get_height()
        ax.text(b.get_x() + b.get_width() / 2, h + 0.1, f"{int(h)}",
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.tick_params(axis='x', labelsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.5)


# ===========================================================================
# Vẽ Subplot (3): Rolling Average Temperature (Cửa sổ trượt 10 mẫu)
# ===========================================================================
def draw_rolling(ax, df: pd.DataFrame, window: int = 10):
    ax.clear()
    ax.set_title(f"Nhiệt độ trung bình trượt — Rolling MA (Cửa sổ = {window} mẫu)", fontsize=10, fontweight='bold')
    ax.set_xlabel("Thứ tự mẫu đo")
    ax.set_ylabel("Nhiệt độ TB (°C)")

    if df.empty or 'temperature' not in df.columns:
        ax.text(0.5, 0.5, "Đang chờ dữ liệu rolling average...",
                ha='center', va='center', transform=ax.transAxes, color='#757575', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.4)
        return

    temp = pd.to_numeric(df['temperature'], errors='coerce').dropna()
    if temp.empty:
        ax.text(0.5, 0.5, "Chưa có dữ liệu nhiệt độ hợp lệ...",
                ha='center', va='center', transform=ax.transAxes, color='#757575')
        ax.grid(True, linestyle='--', alpha=0.4)
        return

    roll = temp.rolling(window=window, min_periods=1).mean()
    ax.plot(range(len(temp)), temp.values, color='#B0BEC5', linewidth=0.9, alpha=0.6, label='Điểm đo thực tế')
    ax.plot(range(len(roll)), roll.values, color='#1565C0', linewidth=2.0, label=f'MA({window})')
    ax.legend(loc='upper right', fontsize=8, framealpha=0.85)

    ax.tick_params(axis='x', labelsize=8)
    ax.grid(True, linestyle='--', alpha=0.5)


# ===========================================================================
# In thống kê ra Console mỗi chu kỳ
# ===========================================================================
def print_stats(state: State, refresh: float):
    now = time.time()
    interval = max(now - state.last_print, 1e-9)
    rate = state.events_since_last / interval

    parts = [f"events/sec={rate:6.1f}",
             f"total={state.total_events:>6d}"]

    # Latency trung bình nếu có
    lat_col = next(
        (c for c in ('latency_us', 'latency', 'lat')
         if not state.df.empty and c in state.df.columns),
        None
    )
    if lat_col:
        try:
            lat_mean = pd.to_numeric(state.df[lat_col], errors='coerce').mean()
            if pd.notna(lat_mean):
                parts.append(f"latency_mean={lat_mean:6.1f} µs")
        except Exception:
            pass

    print("[dashboard] " + "  |  ".join(parts))
    state.events_since_last = 0
    state.last_print = now


# ===========================================================================
# Entry Point
# ===========================================================================
def main():
    setup_matplotlib_backend()

    ap = argparse.ArgumentParser(description="Dashboard realtime cho IoT Stream Framework")
    ap.add_argument('--file', default='dashboard/data.csv',
                    help="Đường dẫn file CSV (mặc định: dashboard/data.csv)")
    ap.add_argument('--refresh', type=float, default=1.0,
                    help="Chu kỳ poll file và cập nhật giao diện, giây (mặc định: 1.0)")
    args = ap.parse_args()

    actual_file = resolve_csv_path(args.file)
    print("======================================================================")
    print("      IoT Stream Framework — Real-Time Telemetry Dashboard")
    print("======================================================================")
    print(f"[*] File giám sát     : {actual_file}")
    print(f"[*] Chu kỳ cập nhật   : {args.refresh}s")
    print("[*] Đang mở cửa sổ trực quan hóa... Nhấn Ctrl+C để dừng.")
    print("======================================================================")

    tail = CSVTail(actual_file)
    state = State()
    state.last_print = time.time()

    fig, axes = plt.subplots(3, 1, figsize=(11, 8.5))
    fig.canvas.manager.set_window_title(f"IoT Stream Telemetry Monitor — {os.path.basename(actual_file)}")
    fig.suptitle("IoT Real-Time Telemetry & Alert Monitoring Dashboard", fontsize=12, fontweight='bold', y=0.98)
    plt.tight_layout(rect=[0.02, 0.02, 0.98, 0.95])

    # Vẽ trạng thái ban đầu ngay khi mở cửa sổ (hiển thị đầy đủ khung, tiêu đề và nhãn)
    draw_temperature(axes[0], state.df)
    draw_severity(axes[1], state.df)
    draw_rolling(axes[2], state.df, window=10)

    def update(_frame):
        # 1. Đọc dữ liệu mới nhất
        new_df = tail.read_new()
        if not new_df.empty:
            state.df = pd.concat([state.df, new_df], ignore_index=True)
            if len(state.df) > State.MAX_ROWS:
                state.df = state.df.iloc[-State.MAX_ROWS:].reset_index(drop=True)
            n_new = len(new_df)
            state.events_since_last += n_new
            state.total_events += n_new

        # 2. Cập nhật lại 3 đồ thị
        draw_temperature(axes[0], state.df)
        draw_severity(axes[1], state.df)
        draw_rolling(axes[2], state.df, window=10)

        # 3. In thống kê ra console
        print_stats(state, args.refresh)
        return axes

    anim = FuncAnimation(
        fig, update,
        interval=max(int(args.refresh * 1000), 200),
        cache_frame_data=False,
    )

    try:
        plt.show()
    except KeyboardInterrupt:
        print("\n[dashboard] Nhận tín hiệu dừng Ctrl+C.")
    finally:
        try:
            anim.event_source.stop()
        except Exception:
            pass
        plt.close(fig)
        print(f"[dashboard] Đã đóng cửa sổ. Tổng số sự kiện ghi nhận: {state.total_events}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n[dashboard] Dừng bởi Ctrl+C.")
