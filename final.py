import os
import cv2
import numpy as np
import json
from datetime import datetime, timezone, timedelta
from ultralytics import YOLO
import paho.mqtt.client as mqtt


# ============== แก้ค่าตรงนี้ได้เลย (หรือ override ผ่าน environment variable เวลารันใน Docker) ==============
SOURCE_PATH = os.getenv("SOURCE_PATH", "data/Train1.mp4")   # พาธไฟล์วิดิโอต้นฉบับ

GENERAL_MODEL_PATH = os.getenv("GENERAL_MODEL_PATH", "yolov8m.pt")  # โมเดล YOLOv8 ทั่วไป (COCO) ใช้ตรวจ คน/รถ/มอไซค์/บัส/รถบรรทุก
TRAFFIC_MODEL_PATH = os.getenv("TRAFFIC_MODEL_PATH", "model/best.pt")  # โมเดลไฟจราจรที่เทรนเอง (red/yellow/green)
TRAFFIC_MODEL_IMGSZ = int(os.getenv("TRAFFIC_MODEL_IMGSZ", "640"))       # ต้องตรงกับ imgsz ตอนเทรน (train.py ใช้ 640)
TRAFFIC_CONF_THRESHOLD = float(os.getenv("TRAFFIC_CONF_THRESHOLD", "0.35"))  # conf ขั้นต่ำเฉพาะโมเดลไฟจราจร ปรับแยกจาก CONF_THRESHOLD ได้

TRACKER_CONFIG = os.getenv("TRACKER_CONFIG", "custom_tracker.yaml")  # ไฟล์ tracker ที่ปรับ track_buffer ไว้ให้แล้ว
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", "0.4"))   # ค่า confidence ขั้นต่ำ (สำหรับโมเดลทั่วไป)
DISPLAY_WIDTH = int(os.getenv("DISPLAY_WIDTH", "1280"))      # ความกว้างของหน้าต่างพรีวิว

# HEADLESS=true (เช่นตอนรันใน Docker ที่ไม่มีจอ/ไม่มี X server) จะปิดหน้าต่างพรีวิว cv2.imshow ไปเลย
HEADLESS = os.getenv("HEADLESS", "false").strip().lower() in ("1", "true", "yes")
# ====================================================

# MQTT
MQTT_BROKER_IP = os.getenv("MQTT_BROKER_IP", "172.16.2.117")     # VerneMQ broker ตาม diagram
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "crosswalk-event-6510301037")

#     (Telegraf connector จะ map ชื่อนี้ไปเป็น Kafka topic ให้)
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "GW_6510301037")   # <-- ตั้งให้ไม่ซ้ำกับคนอื่น เช่นใส่รหัสนักศึกษา
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None   # ใส่ถ้า VerneMQ ต้อง auth ไม่งั้นปล่อย None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
# ============================================


mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

if MQTT_USERNAME:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[MQTT] เชื่อมต่อ VerneMQ สำเร็จ: {MQTT_BROKER_IP}:{MQTT_BROKER_PORT}")
    else:
        print(f"[MQTT] เชื่อมต่อล้มเหลว reason_code={reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[MQTT] หลุดการเชื่อมต่อ reason_code={reason_code}")


mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

try:
    mqtt_client.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT, keepalive=60)
    mqtt_client.loop_start()  # background thread จัดการ network loop / reconnect ให้อัตโนมัติ
except Exception as e:
    print(f"[MQTT] เชื่อมต่อไม่สำเร็จตอนเริ่มโปรแกรม: {e}")


# COCO class ids ที่สนใจ: 0 = person, 2 = car, 3 = motorcycle
# หมายเหตุ: เอา 9 (traffic_light ของ COCO) ออก เพราะเปลี่ยนไปใช้โมเดลไฟจราจรที่เทรนเองแทนแล้ว
TARGET_CLASSES = {0: "person", 2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

# สีกรอบ (BGR) แยกตามคลาส
COLORS = {
    0: (0, 255, 0),     # person -> เขียว
    2: (0, 165, 255),   # car -> ส้ม
    3: (255, 0, 255),   # motorcycle -> ม่วงชมพู
    5: (0, 0, 255),
    7: (255, 0, 0),
}

# class id ของโมเดลไฟจราจรที่เทรนเอง ต้องตรงกับ dataset.yaml (0: red, 1: yellow, 2: green)
TL_CLASS_NAMES = {0: "RED", 1: "YELLOW", 2: "GREEN"}

# กรอบพื้นที่ (ROI) ที่คาดว่าไฟจราจรควรอยู่ในภาพ ใช้กรองกันโมเดลไปจับผิดจุดอื่นในเฟรม
roi = (650, 450, 720, 560)

crosswalk_zone = np.array([
    [700, 660],
    [1444, 725],
    [1467, 660],
    [830, 615]
], dtype=np.int32)

# เส้นล่าง
bottom_line = (
    crosswalk_zone[0],
    crosswalk_zone[1]
)

# เส้นบน
top_line = (
    crosswalk_zone[3],
    crosswalk_zone[2]
)

vehicle_states = {}
violation_ids = set()


def is_box_inside_roi(box, roi):
    x1, y1, x2, y2 = box
    rx1, ry1, rx2, ry2 = roi

    # จุดกึ่งกลางของกรอบ YOLO
    cx = (x1 + x2) / 2
    cy = (y1 + y2) / 2

    # ตรวจว่าจุดกึ่งกลางอยู่ใน ROI หรือไม่
    return rx1 <= cx <= rx2 and ry1 <= cy <= ry2


def line_side(point, line_p1, line_p2):
    x, y = float(point[0]), float(point[1])
    x1, y1 = float(line_p1[0]), float(line_p1[1])
    x2, y2 = float(line_p2[0]), float(line_p2[1])
    return (x2 - x1) * (y - y1) - (y2 - y1) * (x - x1)


def point_within_line_x_range(point, line_p1, line_p2, margin=0):
    """เช็คว่า x ของจุดอยู่ในช่วงความกว้างของ 'เส้นจริง' (ไม่ใช่เส้นที่ลากยาวไม่มีที่สิ้นสุด)

    line_side()/crossed_line() คำนวณแบบเส้นตรงยาวไม่มีที่สิ้นสุด (infinite line) ถ้าไม่เช็คจุดนี้ด้วย
    รถที่วิ่งอยู่คนละเลน ไกลจากทางม้าลายจริงๆ ก็จะถูกนับว่า 'ข้ามเส้น' ไปด้วย ทั้งที่ไม่เคยเข้าใกล้
    ทางม้าลายเลย เพราะแค่เคลื่อนผ่านระดับ y เดียวกับเส้นสมมติที่ลากต่อออกไปนอกกรอบ
    """
    x = point[0]
    x1, x2 = line_p1[0], line_p2[0]
    lo, hi = min(x1, x2) - margin, max(x1, x2) + margin
    return lo <= x <= hi


def crossed_line(
    prev_point,
    current_point,
    line_p1,
    line_p2,
    x_margin=40
):

    prev_side = line_side(
        prev_point,
        line_p1,
        line_p2
    )

    current_side = line_side(
        current_point,
        line_p1,
        line_p2
    )

    sign_flipped = prev_side * current_side < 0

    # ต้องข้าม 'ในช่วงความกว้างของเส้นจริง' เท่านั้น ไม่ใช่แค่เครื่องหมายพลิกที่ไหนก็ได้
    in_range = point_within_line_x_range(current_point, line_p1, line_p2, margin=x_margin)

    return sign_flipped and in_range


def create_violation_payload(
    traffic_state,
    vehicle_id,
    vehicle_type,
    person_count
):
    payload = {
        "timestamp": datetime.now().isoformat(),
        "traffic_state": traffic_state,
        "vehicle": {
            "vehicle_id": vehicle_id,
            "type": vehicle_type,
        },
        "pedestrian": person_count
    }

    return payload

def send_payload(payload):
    """ส่ง payload ไปที่ VerneMQ (MQTT) ทันทีที่ถูกสร้าง แล้ว bridge จะดูดต่อเข้า Kafka/InfluxDB ให้เอง
    และ print ไว้ดูใน console ด้วย"""
    payload_str = json.dumps(payload, ensure_ascii=False)
    print(payload_str)

    try:
        result = mqtt_client.publish(MQTT_TOPIC, payload_str, qos=MQTT_QOS)
        # result.rc == 0 หมายถึงส่งเข้าคิวสำเร็จ (ไม่ได้การันตีว่า broker รับแล้วถ้า QoS=0)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[MQTT] publish ล้มเหลว rc={result.rc}")
    except Exception as e:
        print(f"[MQTT] publish error: {e}")

def point_in_crosswalk(point, crosswalk_zone):
    return cv2.pointPolygonTest(
        crosswalk_zone,
        point,
        False
    ) >= 0


def detect_traffic_light(traffic_model, frame, roi):
    """ตรวจไฟจราจรด้วยโมเดลที่เทรนเอง คืนค่า (box, color) หรือ (None, None) ถ้าไม่เจอในเฟรมนี้

    โมเดลนี้เทรนมาให้ตรวจจับ 'กล่องไฟจราจรทั้งกล่อง' (ครบ 3 ดวง) แล้วบอกสีที่ติดอยู่ตรงๆ
    ผ่าน class (0=red, 1=yellow, 2=green) จึงไม่ต้องพึ่งการตรวจสีด้วย HSV แยกอีกขั้นแบบเดิม
    """
    results = traffic_model.predict(
        source=frame,
        conf=TRAFFIC_CONF_THRESHOLD,
        imgsz=TRAFFIC_MODEL_IMGSZ,
        verbose=False,
    )[0]

    if results.boxes is None or len(results.boxes) == 0:
        return None, None

    # ปกติควรเจอกล่องเดียว (โมเดลเทรนมาสำหรับไฟจราจรจุดเดียว) เผื่อเจอหลายกล่องให้เลือกที่ conf สูงสุด
    best_box = max(results.boxes, key=lambda b: float(b.conf[0]))
    x1, y1, x2, y2 = map(int, best_box.xyxy[0])
    cls_id = int(best_box.cls[0])

    if not is_box_inside_roi((x1, y1, x2, y2), roi):
        return None, None

    color = TL_CLASS_NAMES.get(cls_id, "UNKNOWN")
    return (x1, y1, x2, y2), color


def main():
    general_model = YOLO(GENERAL_MODEL_PATH)
    traffic_model = YOLO(TRAFFIC_MODEL_PATH)

    # เก็บค่าล่าสุดที่เจอไว้ข้ามเฟรม เผื่อบางเฟรมตรวจไฟจราจรไม่เจอ (กล้องขยับ/เบลอ)
    # จะได้ไม่กระพริบ UNKNOWN สลับไปมา ใช้ค่าล่าสุดที่เจอจริงไปก่อนจนกว่าจะเจอใหม่
    traffic_light_box = None
    traffic_color = "UNKNOWN"

    cap = cv2.VideoCapture(SOURCE_PATH)
    if not cap.isOpened():
        raise IOError(f"เปิดวิดิโอไม่ได้: {SOURCE_PATH}")

    orig_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    orig_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    scale = DISPLAY_WIDTH / orig_width if orig_width > DISPLAY_WIDTH else 1.0
    display_height = int(orig_height * scale)

    while True:
        ret, frame = cap.read()

        if not ret:
            # วิดีโอจบ → กลับไปต้นคลิป
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ret, frame = cap.read()

            # ล้างสถานะรถจากรอบก่อน
            vehicle_states.clear()
            violation_ids.clear()

            if not ret:
                print("ไม่สามารถอ่านวิดีโอได้")
                break

            continue

        original_frame = frame.copy()
        display_frame = frame.copy()

        # ---- 1) ตรวจไฟจราจรด้วยโมเดลที่เทรนเอง (แยกโมเดลจากคน/รถ) ----
        found_box, found_color = detect_traffic_light(traffic_model, original_frame, roi)
        if found_box is not None:
            traffic_light_box = found_box
            traffic_color = found_color
        # ถ้าเฟรมนี้ไม่เจอ ใช้ traffic_light_box / traffic_color ของเฟรมก่อนหน้าต่อไปก่อน

        # ---- 2) ตรวจคน/รถ ด้วยโมเดลทั่วไป (คนละโมเดลกับไฟจราจร) ----
        # ใช้ model.track แทน model.predict เพื่อให้ได้ ID ต่อเนื่องของแต่ละวัตถุ
        # persist=True บอกให้ tracker จำสถานะจากเฟรมก่อนหน้าไว้ใช้ต่อ
        results = general_model.track(
            source=original_frame,
            classes=list(TARGET_CLASSES.keys()),
            conf=CONF_THRESHOLD,
            tracker=TRACKER_CONFIG,
            persist=True,
            verbose=False,
        )

        result = results[0]
        counts = {"person": 0, "car": 0, "motorcycle": 0, "bus": 0, "truck": 0}
        crosswalk_person_count = 0

        if result.boxes is not None:

            for box in result.boxes:

                cls_id = int(box.cls[0])
                conf = float(box.conf[0])
                x1, y1, x2, y2 = map(int, box.xyxy[0])

                # box.id เป็น None ได้ในเฟรมแรกๆ ก่อน tracker จะ assign ID สำเร็จ
                track_id = int(box.id[0]) if box.id is not None else -1

                label = TARGET_CLASSES.get(cls_id, str(cls_id))
                color = COLORS.get(cls_id, (255, 255, 255))
                counts[label] = counts.get(label, 0) + 1

                if label == "person":
                    bottom_center = (
                        int((x1 + x2) / 2),
                        y2
                    )

                    if point_in_crosswalk(bottom_center, crosswalk_zone):
                        crosswalk_person_count += 1

                cv2.rectangle(display_frame, (x1, y1), (x2, y2), color, 2)
                id_text = f"ID:{track_id}" if track_id != -1 else "ID:.."
                text = f"{id_text} {label} {conf:.2f}"
                (tw, th), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
                cv2.rectangle(display_frame, (x1, y1 - th - 6), (x1 + tw + 4, y1), color, -1)
                cv2.putText(display_frame, text, (x1 + 2, y1 - 4),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1, cv2.LINE_AA)

                if traffic_color in ["RED", "YELLOW"]:
                    cv2.polylines(display_frame, [crosswalk_zone], True, (0, 255, 0), 2)

                    if label in ["car", "motorcycle", "bus", "truck"] and track_id != -1:
                        current_point = (
                            int((x1 + x2) / 2),
                            y2
                        )
                        if track_id not in vehicle_states:
                            vehicle_states[track_id] = {
                                "prev_point": current_point,
                                "crossed_top": False,
                                "crossed_bottom": False,
                                "violation": False
                            }

                        state = vehicle_states[track_id]
                        prev_point = state["prev_point"]

                        # ตรวจเส้นบน
                        if not state["crossed_top"]:
                            if crossed_line(
                                prev_point,
                                current_point,
                                top_line[0],
                                top_line[1]
                            ):
                                state["crossed_top"] = True
                                print(f"ID {track_id} ผ่านเส้นบน")

                        # ตรวจเส้นล่าง
                        if not state["crossed_bottom"]:
                            if crossed_line(
                                prev_point,
                                current_point,
                                bottom_line[0],
                                bottom_line[1]
                            ):
                                state["crossed_bottom"] = True
                                print(f"ID {track_id} ผ่านเส้นล่าง")

                        # ผ่านทั้ง 2 เส้น = ฝ่าทางม้าลาย
                        if (
                            state["crossed_top"]
                            and state["crossed_bottom"]
                            and not state["violation"]
                        ):
                            state["violation"] = True

                            if track_id not in violation_ids:
                                violation_ids.add(track_id)
                                payload = create_violation_payload(traffic_color, track_id, label, crosswalk_person_count)
                                send_payload(payload)

                        # อัปเดตตำแหน่งเดิม
                        state["prev_point"] = current_point

        if traffic_light_box is not None:
            tlx1, tly1, tlx2, tly2 = traffic_light_box
            tl_draw_color = {
                "RED": (0, 0, 255),
                "YELLOW": (0, 255, 255),
                "GREEN": (0, 255, 0),
            }.get(traffic_color, (255, 255, 255))
            cv2.rectangle(display_frame, (tlx1, tly1), (tlx2, tly2), tl_draw_color, 2)
            cv2.putText(display_frame, traffic_color, (tlx1, max(0, tly1 - 6)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, tl_draw_color, 2, cv2.LINE_AA)

        display_frame = cv2.resize(display_frame, (DISPLAY_WIDTH, display_height))

        # HEADLESS=true (เช่นรันใน Docker ไม่มีจอ) จะข้ามการเปิดหน้าต่างพรีวิวไปเลย
        if not HEADLESS:
            cv2.imshow("YOLOv8 Detection + Tracking", display_frame)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

    cap.release()
    if not HEADLESS:
        cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
