"""
MQTT -> Kafka + InfluxDB bridge

ทำหน้าที่แทน Telegraf: subscribe MQTT topic แล้ว
  1) ส่งต่อข้อความ "ดิบๆ" เข้า Kafka topic ปลายทางทันที (ไม่แกะ/แปลงอะไรเลย เหมือนเดิม)
  2) แกะ JSON แล้วเขียนเป็น data point เข้า InfluxDB (เอาไป plot กราฟ/ทำ dashboard ต่อได้)

วิธีติดตั้ง:
    pip install paho-mqtt kafka-python influxdb-client

หมายเหตุ: โค้ดนี้ใช้ InfluxDB 2.x (แพ็กเกจ influxdb-client) ถ้าใช้ InfluxDB 1.x (เช่น 1.8)
ต้องเปลี่ยนไปใช้แพ็กเกจ `influxdb` (InfluxDBClient แบบเก่า) แทน โครงสร้าง client จะต่างจากนี้
พอสมควร (ไม่มี org/bucket, ใช้ database + measurement ผ่าน dict แทน Point object) บอกได้ถ้าใช้
เวอร์ชันนั้น จะปรับโค้ดให้ใหม่

วิธีใช้งาน:
    python mqtt_kafka_influx_bridge.py

รันทิ้งไว้เป็น background process ตลอดเวลาที่ต้องการให้ bridge ทำงาน
(รันคู่ขนานไปกับสคริปต์ตรวจจับ traffic_violation_detector.py)
"""

import os
import json
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ============== ตั้งค่า MQTT (ฝั่งรับ - VerneMQ) ==============
# ค่าทั้งหมดอ่านจาก environment variable ก่อน ถ้าไม่ตั้งไว้จะ fallback ไปใช้ค่า default ด้านหลัง
# (ออกแบบไว้ให้ override ผ่าน .env / docker-compose ได้โดยไม่ต้องแก้โค้ด)
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "172.16.2.117")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "crosswalk-event-6510301037")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-kafka-bridge-6510301037")
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None

# ============== ตั้งค่า Kafka (ปลายทางที่ 1 - ส่งดิบผ่านตรง) ==============
KAFKA_BROKER_IP = os.getenv("KAFKA_BROKER_IP", "172.16.2.117")
KAFKA_BROKER_PORT = int(os.getenv("KAFKA_BROKER_PORT", "9092"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crosswalk-event-6510301037")

# ============== ตั้งค่า InfluxDB (ปลายทางที่ 2 - เก็บเป็น time-series) ==============
# INFLUX_TOKEN ไม่ควร hardcode ในไฟล์นี้ (เป็นความลับ) - ตั้งผ่าน environment variable เสมอ
# ในเครื่อง: export INFLUX_TOKEN=xxxx (Linux/Mac) หรือ $env:INFLUX_TOKEN="xxxx" (PowerShell)
# ใน Docker: ใส่ใน .env แล้วอ้างใน docker-compose.yml (ดู .env.example)
INFLUX_URL = os.getenv("INFLUX_URL", "http://172.16.2.117:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "YOUR_INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "mini_project")
INFLUX_MEASUREMENT = os.getenv("INFLUX_MEASUREMENT", "crosswalk_6510301037")
# ================================================================

kafka_producer = None
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=[f"{KAFKA_BROKER_IP}:{KAFKA_BROKER_PORT}"],
        # ไม่ใส่ value_serializer ใดๆ - ส่ง bytes ดิบตรงๆ ไม่แปลงอะไรทั้งสิ้น (เหมือนเดิมทุกประการ)
        max_block_ms=8000,
        request_timeout_ms=8000,
        api_version=(2, 5, 0),
    )
    print(f"[Kafka] เชื่อมต่อสำเร็จ: {KAFKA_BROKER_IP}:{KAFKA_BROKER_PORT}")
except KafkaError as e:
    print(f"[Kafka] เชื่อมต่อไม่สำเร็จ: {e}")
    raise

influx_client = None
influx_write_api = None
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print(f"[InfluxDB] เชื่อมต่อสำเร็จ: {INFLUX_URL} (bucket={INFLUX_BUCKET})")
except Exception as e:
    print(f"[InfluxDB] เชื่อมต่อไม่สำเร็จ: {e}")
    raise


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[MQTT] เชื่อมต่อ VerneMQ สำเร็จ: {MQTT_BROKER_IP}:{MQTT_BROKER_PORT}")
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[MQTT] subscribe topic: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] เชื่อมต่อล้มเหลว reason_code={reason_code}")


def build_influx_point(payload: dict) -> Point:
    """แปลง payload (dict จาก JSON ที่ฝั่งตรวจจับส่งมา) ให้เป็น Point สำหรับเขียนเข้า InfluxDB

    - traffic_state / vehicle type -> tag (เอาไว้ filter/group by ตอน query)
    - vehicle_id / pedestrian count -> field (ค่าตัวเลขที่จะเอาไปพล็อตกราฟ/รวมยอด)
    - เวลาที่ใช้บันทึกใช้ payload["timestamp"] (เวลาที่เหตุการณ์เกิดจริงฝั่งตรวจจับ)
      ไม่ใช้เวลาที่ bridge ประมวลผลข้อความ กันความคลาดเคลื่อนจากดีเลย์เครือข่าย/คิว
    """
    vehicle = payload.get("vehicle", {}) or {}

    point = (
        Point(INFLUX_MEASUREMENT)
        .tag("traffic_state", payload.get("traffic_state", "UNKNOWN"))
        .tag("vehicle_type", vehicle.get("type", "unknown"))
        .field("vehicle_id", int(vehicle.get("vehicle_id", -1)))
        .field("pedestrian_count", int(payload.get("pedestrian", 0)))
    )

    ts = payload.get("timestamp")
    if ts:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            # payload สร้างจาก datetime.now().isoformat() (ไม่มี timezone) ฝั่งตรวจจับ
            # ถือว่าเป็นเวลาท้องถิ่นของเครื่องที่รัน ปรับ timezone ตรงนี้ถ้าเครื่องรันคนละ tz กับที่ต้องการ
            dt = dt.replace(tzinfo=timezone.utc)
        point = point.time(dt, WritePrecision.NS)

    return point


def on_message(client, userdata, msg):
    """ทุกข้อความที่ได้จาก MQTT:
    1) ส่งต่อเข้า Kafka แบบดิบๆ ทันที (พฤติกรรมเดิม ไม่เปลี่ยนแปลง)
    2) แกะ JSON แล้วเขียนเข้า InfluxDB แยกต่างหาก
       (ถ้า parse/เขียนไม่สำเร็จ แค่ log ไว้ ไม่ทำให้ฝั่ง Kafka สะดุดหรือหยุดทำงาน)
    """
    raw_payload = msg.payload  # bytes ดิบเป๊ะๆ ตามที่ publish เข้ามา

    print(f"[MQTT -> Kafka] {raw_payload.decode('utf-8', errors='replace')}")

    # ---- 1) ส่งต่อเข้า Kafka แบบดิบ (เหมือนเดิมทุกประการ) ----
    try:
        future = kafka_producer.send(KAFKA_TOPIC, value=raw_payload)
        future.get(timeout=8)  # รอผลจริง กันเงียบๆ หายไปโดยไม่รู้ตัว
    except KafkaError as e:
        print(f"[Kafka ERROR] ส่งไม่สำเร็จ: {e}")

    # ---- 2) แกะ JSON แล้วเขียนเข้า InfluxDB ----
    try:
        payload = json.loads(raw_payload)
        point = build_influx_point(payload)
        influx_write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(f"[InfluxDB] เขียนสำเร็จ: {INFLUX_MEASUREMENT}")
    except json.JSONDecodeError as e:
        print(f"[InfluxDB ERROR] payload ไม่ใช่ JSON ที่ถูกต้อง: {e}")
    except Exception as e:
        print(f"[InfluxDB ERROR] เขียนไม่สำเร็จ: {e}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[MQTT] หลุดการเชื่อมต่อ reason_code={reason_code}")


def main():
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT, keepalive=60)
    print("[Bridge] เริ่มทำงาน... กด Ctrl+C เพื่อหยุด")

    try:
        client.loop_forever()  # บล็อกรันตลอดไป จนกว่าจะกด Ctrl+C
    except KeyboardInterrupt:
        print("\n[Bridge] กำลังปิด...")
    finally:
        client.disconnect()
        kafka_producer.flush()
        kafka_producer.close()
        if influx_client:
            influx_client.close()


if __name__ == "__main__":
    main()
