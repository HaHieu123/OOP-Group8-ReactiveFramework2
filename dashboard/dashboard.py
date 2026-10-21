#!/usr/bin/env python3
"""
dashboard.py — Dashboard realtime đọc CSV do FileSink ghi ra.

Đặc điểm:
- Tail-style: poll file mỗi `--refresh` giây, chỉ đọc phần mới (byte offset),
  bỏ qua dòng cuối chưa hoàn chỉnh (chưa có '\n').
- 3 subplot qua FuncAnimation:
    (1) Line: temperature theo thời gian, một đường mỗi sensor
    (2) Bar : phân bố severity INFO/WARN/ERROR
    (3) Line: rolling average temperature (window=10 điểm)
- Console: events/sec + latency trung bình (nếu có cột latency).
- Ctrl+C dừng sạch (đóng figure, in tổng kết).

Chạy:
    python dashboard.py --file dashboard/data.csv --refresh 1
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
# Tail CSV: đọc phần mới của file kể từ offset đã đọc
# ===========================================================================
class CSVTail:
    """Đọc file CSV theo kiểu `tail -f` — chỉ trả về những dòng mới."""

    def __init__(self, path: str):
        self.path = path
        self.offset = 0          # byte offset đã đọc xong
        self.columns = None      # header cache

    def read_new(self) -> pd.DataFrame:
        if not os.path.exists(self.path):
            return pd.DataFrame()

        try:
            with open(self.path, 'rb') as f:
                f.seek(self.offset)
                data = f.read()
        except OSError:
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        # Chỉ xử lý tới newline cuối cùng — bỏ dòng cuối nếu bị cắt giữa chừng
        last_nl = data.rfind(b'\n')
        if last_nl == -1:
            return pd.DataFrame()        # chưa có dòng hoàn chỉnh
        complete = data[:last_nl + 1]
        self.offset += len(complete)
        text = complete.decode('utf-8', errors='replace')

        # Lần đầu: trích header, phần còn lại là dữ liệu
        if self.columns is None:
            lines = text.splitlines()
            if not lines:
                return pd.DataFrame()
            self.columns = [c.strip() for c in lines[0].split(',')]
            rest = '\n'.join(lines[1:])
            if not rest.strip():
                return pd.DataFrame()
            buf = io.StringIO(rest)
        else:
            buf = io.StringIO(text)

        try:
            df = pd.read_csv(buf, header=None, names=self.columns)
        except Exception as exc:
            print(f"[dashboard] Lỗi parse CSV: {exc}", file=sys.stderr)
            return pd.DataFrame()
        return df


# ===========================================================================
# State dùng chung giữa các frame
# ===========================================================================
class State:
    MAX_ROWS = 5000      # giới hạn bộ nhớ: chỉ giữ 5000 dòng gần nhất

    def __init__(self):
        self.df = pd.DataFrame()
        self.events_since_last = 0
        self.total_events = 0
        self.last_print = 0.0


# ===========================================================================
# Vẽ subplot (a): line temperature theo thời gian, mỗi sensor một đường
# ===========================================================================
def draw_temperature(ax, df: pd.DataFrame):
    ax.clear()
    ax.set_title("Temperature theo thời gian (per sensor)")
    ax.set_ylabel("Nhiệt độ (°C)")

    if df.empty or 'timestamp' not in df.columns or 'temperature' not in df.columns:
        ax.text(0.5, 0.5, "Chưa có dữ liệu", ha='center', va='center',
                transform=ax.transAxes)
        return

    work = df.copy()
    work['timestamp'] = pd.to_datetime(work['timestamp'], errors='coerce')
    work['temperature'] = pd.to_numeric(work['temperature'], errors='coerce')
    work = work.dropna(subset=['timestamp', 'temperature']).sort_values('timestamp')
    if work.empty:
        ax.text(0.5, 0.5, "Chưa có dữ liệu hợp lệ", ha='center', va='center',
                transform=ax.transAxes)
        return

    if 'sensor_id' in work.columns:
        for sensor, grp in work.groupby('sensor_id'):
            ax.plot(grp['timestamp'], grp['temperature'],
                    label=str(sensor), linewidth=1.2)
        ax.legend(loc='upper right', fontsize=8, framealpha=0.7)
    else:
        ax.plot(work['timestamp'], work['temperature'], linewidth=1.2)

    ax.tick_params(axis='x', labelrotation=25, labelsize=8)


# ===========================================================================
# Vẽ subplot (b): bar chart phân bố severity
# ===========================================================================
def draw_severity(ax, df: pd.DataFrame):
    ax.clear()
    ax.set_title("Phân bố severity")
    ax.set_ylabel("Số event")

    if df.empty or 'severity' not in df.columns:
        ax.text(0.5, 0.5, "Chưa có dữ liệu", ha='center', va='center',
                transform=ax.transAxes)
        return

    counts = df['severity'].astype(str).value_counts()
    # Sắp xếp có thứ tự ưu tiên: INFO, WARN, ERROR rồi các giá trị lạ
    order = [s for s in ('INFO', 'WARN', 'ERROR') if s in counts.index]
    order += [s for s in counts.index if s not in order]
    colors = {'INFO': '#4CAF50', 'WARN': '#FFC107', 'ERROR': '#F44336'}

    bars = ax.bar(order, [counts[s] for s in order],
                  color=[colors.get(s, '#90A4AE') for s in order])
    # Ghi số trên đầu mỗi cột
    for b in bars:
        h = b.get_height()
        ax.text(b.get_x() + b.get_width() / 2, h, f"{int(h)}",
                ha='center', va='bottom', fontsize=8)
    ax.tick_params(axis='x', labelsize=9)


# ===========================================================================
# Vẽ subplot (c): rolling average temperature, window = 10 điểm
# ===========================================================================
def draw_rolling(ax, df: pd.DataFrame, window: int = 10):
    ax.clear()
    ax.set_title(f"Rolling avg temperature (window={window})")
    ax.set_xlabel("Điểm dữ liệu")
    ax.set_ylabel("Nhiệt độ TB (°C)")

    if df.empty or 'temperature' not in df.columns:
        ax.text(0.5, 0.5, "Chưa có dữ liệu", ha='center', va='center',
                transform=ax.transAxes)
        return

    temp = pd.to_numeric(df['temperature'], errors='coerce').dropna()
    if temp.empty:
        ax.text(0.5, 0.5, "Chưa có dữ liệu hợp lệ", ha='center', va='center',
                transform=ax.transAxes)
        return

    roll = temp.rolling(window=window, min_periods=1).mean()
    ax.plot(range(len(roll)), roll.values, color='#1976D2', linewidth=1.5)
    ax.tick_params(axis='x', labelsize=8)


# ===========================================================================
# In thống kê ra console
# ===========================================================================
def print_stats(state: State, refresh: float):
    now = time.time()
    interval = max(now - state.last_print, 1e-9)
    rate = state.events_since_last / interval

    parts = [f"events/sec={rate:8.1f}",
             f"total={state.total_events:>8d}"]

    # Latency trung bình nếu có cột phù hợp
    lat_col = next(
        (c for c in ('latency_us', 'latency', 'lat')
         if not state.df.empty and c in state.df.columns),
        None,
    )
    if lat_col:
        try:
            lat_mean = pd.to_numeric(state.df[lat_col], errors='coerce').mean()
            if pd.notna(lat_mean):
                parts.append(f"latency_mean={lat_mean:7.1f} µs")
        except Exception:
            pass

    print("[dashboard] " + "  ".join(parts))
    state.events_since_last = 0
    state.last_print = now


# ===========================================================================
# main
# ===========================================================================
def main():
    ap = argparse.ArgumentParser(description="Dashboard realtime cho FileSink CSV")
    ap.add_argument('--file', default='dashboard/data.csv',
                    help="Đường dẫn CSV (default: dashboard/data.csv)")
    ap.add_argument('--refresh', type=float, default=1.0,
                    help="Chu kỳ poll file, giây (default: 1)")
    args = ap.parse_args()

    # Cố gắng dùng backend tương tác; nếu không có sẽ fallback mặc định
    try:
        matplotlib.use('TkAgg')
    except Exception:
        pass

    tail = CSVTail(args.file)
    state = State()
    state.last_print = time.time()

    fig, axes = plt.subplots(3, 1, figsize=(11, 8))
    fig.suptitle(f"iot_stream dashboard — {args.file}", fontsize=12)
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    def update(_frame):
        # 1) Đọc dữ liệu mới + append vào buffer
        new_df = tail.read_new()
        if not new_df.empty:
            state.df = pd.concat([state.df, new_df], ignore_index=True)
            if len(state.df) > State.MAX_ROWS:
                state.df = state.df.iloc[-State.MAX_ROWS:].reset_index(drop=True)
            n_new = len(new_df)
            state.events_since_last += n_new
            state.total_events += n_new

        # 2) Vẽ lại cả 3 subplot khi đã có ít nhất 1 event
        if state.total_events > 0:
            draw_temperature(axes[0], state.df)
            draw_severity(axes[1], state.df)
            draw_rolling(axes[2], state.df, window=10)

        # 3) In thống kê console
        print_stats(state, args.refresh)
        return axes

    anim = FuncAnimation(
        fig, update,
        interval=int(args.refresh * 1000),
        cache_frame_data=False,
    )

    try:
        plt.show()
    except KeyboardInterrupt:
        print("\n[dashboard] Dừng bởi Ctrl+C")
    finally:
        try:
            anim.event_source.stop()
        except Exception:
            pass
        plt.close(fig)
        print(f"[dashboard] Kết thúc. Tổng events đã đọc: {state.total_events}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        # Lưới an toàn nếu Ctrl+C xảy ra trước khi plt.show()
        print("\n[dashboard] Dừng bởi Ctrl+C (outer)")