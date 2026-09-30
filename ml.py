
import os
import sys
from datetime import datetime, timedelta
import time
import numpy as np
import pandas as pd

# หมายเหตุ: import prophet และ influxdb_client แบบ lazy (เฉพาะตอนใช้งานจริง)
# เพื่อให้โหมด "mock" รันทดสอบ logic ได้แม้เครื่องยังไม่ได้ติดตั้ง library เหล่านี้


# ==================================================================
# 1) ส่วนตั้งค่า - แก้ตรงนี้ให้ตรงกับระบบจริง
# ==================================================================
MODE = "real"  # เปลี่ยนเป็น "real" เมื่อข้อมูลสะสมพอแล้ว
    
INFLUX_URL = os.getenv("INFLUX_URL", "http://172.16.2.117:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "YOUR_INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "mini_project")
INFLUX_MEASUREMENT = os.getenv("INFLUX_MEASUREMENT", "crosswalk_6510301037")

# สำคัญ: WINDOW กับ FORECAST_FREQ ต้องเป็น "หน่วยเดียวกันเสมอ" ไม่งั้น Prophet จะพยากรณ์
# จุดที่ระยะห่างไม่ตรงกับข้อมูล training ผลลัพธ์จะเพี้ยน (ที่เจอค่า 566.9m / 942m ก่อนหน้านี้ก็มาจากจุดนี้)
# แก้ไขให้กันพลาด: ตั้งแค่ WINDOW ตัวเดียว แล้วให้โค้ดคำนวณ FORECAST_FREQ ให้เองด้านล่าง (ไม่ต้องตั้ง FORECAST_FREQ เอง)
RANGE_START = "-1h"    # ดึงข้อมูลย้อนหลังมา train
WINDOW = "2m"          # ขนาด window นับจำนวน violation ต่อช่วง (ตัวเดียวที่ต้องตั้ง)
MIN_RECORDS_FOR_REAL = 30      # ขั้นต่ำที่ควรมีก่อนสลับไปโหมด real จริงจัง

FORECAST_PERIODS = 1   # ทำนายล่วงหน้ากี่ช่วง (1 ช่วง = 1 x WINDOW ถัดไป)


def _flux_window_to_pandas_freq(window: str) -> str:
    """แปลงหน่วย window แบบ Flux (เช่น '1m', '2m', '30m', '1h') เป็นหน่วยที่ pandas/Prophet เข้าใจ
    เพื่อการันตีว่า FORECAST_FREQ จะตรงกับ WINDOW เสมอ ไม่ต้องตั้งแยกกันเองอีกต่อไป
    (จุดนี้คือจุดที่พลาดซ้ำมาหลายรอบ เลยตัดปัญหาด้วยการคำนวณอัตโนมัติแทน)"""
    import re
    match = re.match(r"^(\d+)([a-zA-Z]+)$", window.strip())
    if not match:
        raise ValueError(f"WINDOW format ไม่ถูกต้อง: '{window}' (ตัวอย่างที่ถูกต้อง: '1m', '2m', '30m', '1h')")
    amount, unit = match.groups()
    unit_map = {"s": "s", "m": "min", "h": "h", "d": "D", "w": "W"}
    if unit not in unit_map:
        raise ValueError(f"หน่วยเวลาไม่รู้จัก: '{unit}' ใน WINDOW='{window}'")
    return f"{amount}{unit_map[unit]}"


FORECAST_FREQ = _flux_window_to_pandas_freq(WINDOW)  # คำนวณอัตโนมัติจาก WINDOW เสมอ ไม่ต้องแก้เอง


# ==================================================================
# 2) ดึงข้อมูลจาก InfluxDB (โหมด real)
# ==================================================================
def fetch_real_data():
    from influxdb_client import InfluxDBClient  # import ตรงนี้ เฉพาะตอนโหมด real เรียกใช้จริง

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {RANGE_START})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
      |> filter(fn: (r) => r["_field"] == "vehicle_type_value")
      |> aggregateWindow(every: {WINDOW}, fn: count, createEmpty: true)
    '''

    df = query_api.query_data_frame(flux_query)
    client.close()

    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()

    if df.empty:
        return pd.DataFrame(columns=["ds", "y"])

    df = df[["_time", "_value"]].rename(columns={"_time": "ds", "_value": "y"})
    df["y"] = df["y"].fillna(0)
    # InfluxDB คืนเวลาเป็น UTC เสมอ ต้องแปลงเป็นเวลาไทย (UTC+7) ก่อนตัด timezone info ทิ้ง
    # ไม่งั้นตัวเลขเวลาจะยังเป็น UTC อยู่ (ช้ากว่าเวลาไทยจริง 7 ชั่วโมง) แค่ไม่มี label บอกว่าเป็น UTC แล้ว
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_convert("Asia/Bangkok").dt.tz_localize(None)
    return df


# ==================================================================
# 3) สร้างข้อมูลจำลอง (โหมด mock) - ใช้ทดสอบ pipeline เท่านั้น
#    ไม่ใช่ข้อมูลจริง ห้ามใช้ตีความผลลัพธ์จริงจัง
# ==================================================================
def generate_mock_data(n_points=180):
    """จำลอง violation count ต่อนาที ให้มี pattern ซ้ำ (loop คล้ายวิดีโอวนซ้ำ) เพื่อทดสอบว่า
    Prophet จับ seasonality/pattern ที่เกิดซ้ำได้จริงหรือไม่"""
    rng = np.random.default_rng(seed=42)
    now = datetime.now()

    # จำลอง loop ทุก 5 นาที (เช่น รถ 2 คันผ่านซ้ำจังหวะเดิมทุกรอบวิดีโอ) + noise เล็กน้อย
    loop_pattern = np.array([2, 2, 1, 2, 3])  # ตัวอย่าง pattern ที่ซ้ำทุก 5 นาที
    base = np.tile(loop_pattern, n_points // len(loop_pattern) + 1)[:n_points].astype(float)
    noise = rng.normal(0, 0.15, size=n_points)
    base = np.clip(base + noise, 0, None)

    timestamps = [now - timedelta(minutes=(n_points - i)) for i in range(n_points)]
    return pd.DataFrame({"ds": timestamps, "y": base})


# ==================================================================
# 4) เทรนโมเดลและพยากรณ์ล่วงหน้า
# ==================================================================
def run_forecast(df: pd.DataFrame) -> pd.DataFrame:
    from prophet import Prophet  # import ตรงนี้ เฉพาะตอนใช้งานจริง

    df = df[["ds", "y"]].copy()
    df["ds"] = pd.to_datetime(df["ds"])

    model = Prophet(
        daily_seasonality=False,   # ข้อมูลช่วงสั้น (นาที) ยังไม่พอเห็น pattern รายวันจริง ปิดไว้ก่อน
        weekly_seasonality=False,  # เช่นเดียวกัน เปิดได้เมื่อมีข้อมูลสะสมหลายสัปดาห์
        yearly_seasonality=False,
        changepoint_prior_scale=0.05,
    )
    model.fit(df)

    future = model.make_future_dataframe(periods=FORECAST_PERIODS, freq=FORECAST_FREQ)
    forecast = model.predict(future)

    # ตัดค่าติดลบทิ้ง เพราะจำนวน violation เป็นลบไม่ได้ในความเป็นจริง
    for col in ["yhat", "yhat_lower", "yhat_upper"]:
        forecast[col] = forecast[col].clip(lower=0)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]


# ==================================================================
# 5) เขียนผลลัพธ์กลับ InfluxDB (เฉพาะส่วนที่เป็นอนาคต ยังไม่เกิดขึ้นจริง)
# ==================================================================
def write_results(forecast_df: pd.DataFrame, history_len: int):
    future_only = forecast_df.tail(FORECAST_PERIODS)

    if MODE == "mock":
        print("[MODE=mock] ข้ามการเขียนจริงลง InfluxDB — พิมพ์ผลลัพธ์ให้ดูแทน")
        print(future_only.head(10).to_string(index=False))
        return

    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for _, row in future_only.iterrows():
        # row["ds"] ตอนนี้เป็นเวลาไทย (naive) ต้อง localize กลับเป็น Asia/Bangkok แล้วแปลงเป็น UTC
        # ก่อนเขียนลง InfluxDB เพราะ InfluxDB เก็บเวลาเป็น UTC เสมอ ไม่งั้นจุดที่เขียนจะเพี้ยนไป 7 ชั่วโมง
        ds_utc = row["ds"].tz_localize("Asia/Bangkok").tz_convert("UTC")
        point = (
            Point(RESULT_MEASUREMENT)
            .field("yhat", float(row["yhat"]))
            .field("yhat_lower", float(row["yhat_lower"]))
            .field("yhat_upper", float(row["yhat_upper"]))
            .time(ds_utc, WritePrecision.NS)
        )
        points.append(point)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    client.close()
    print(f"[InfluxDB] เขียนค่าพยากรณ์สำเร็จ: {len(points)} points -> measurement '{RESULT_MEASUREMENT}'")


# ==================================================================
# 6) main
# ==================================================================
def main():
    print(f"[START] โหมดที่ใช้งาน: {MODE}")

    if MODE == "mock":
        df = generate_mock_data()
        print(f"[MOCK] สร้างข้อมูลจำลอง {len(df)} จุด (มี pattern ซ้ำทุก 5 นาที จำลองวิดีโอ loop)")
    elif MODE == "real":
        df = fetch_real_data()
        print(f"[REAL] ดึงข้อมูลจริงจาก InfluxDB ได้ {len(df)} จุด")
        if len(df) < MIN_RECORDS_FOR_REAL:
            print(
                f"[WARNING] ข้อมูลยังมีแค่ {len(df)} จุด (แนะนำอย่างน้อย {MIN_RECORDS_FOR_REAL}) "
                "ผลพยากรณ์ที่ได้อาจไม่แม่นยำ ลองรอสะสมข้อมูลเพิ่มก่อน"
            )
        if df.empty:
            print("[ERROR] ไม่พบข้อมูลใน InfluxDB ตรวจสอบ bucket/measurement/ช่วงเวลาที่ query อีกครั้ง")
            sys.exit(1)
    else:
        print(f"[ERROR] ไม่รู้จัก MODE = '{MODE}' (ต้องเป็น 'mock' หรือ 'real')")
        sys.exit(1)

    forecast_df = run_forecast(df)
    print(f"[RESULT] พยากรณ์ล่วงหน้า {FORECAST_PERIODS} ช่วงเวลาสำเร็จ")

    # ดึงเฉพาะ "ช่วงเวลาถัดไปช่วงแรก" (เช่น 1 นาทีข้างหน้า ถ้า WINDOW = 1m) มาโชว์แยกให้ชัดเจน
    next_period = forecast_df.tail(FORECAST_PERIODS).iloc[0]
    print(
        f"[NEXT PERIOD] เวลา {next_period['ds']} คาดว่าจะมี violation "
        f"~{next_period['yhat']:.1f} ครั้ง (ช่วงที่เป็นไปได้: {next_period['yhat_lower']:.1f} - {next_period['yhat_upper']:.1f})"
    )

    write_results(forecast_df, history_len=len(df))
    print("[DONE]")


if __name__ == "__main__":
    while True:
        main()
        time.sleep(120)