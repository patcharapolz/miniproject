# System Architecture Diagram

**Edge-to-Cloud Real-Time Smart Traffic Analytics Platform**
โครงการตรวจจับการฝ่าไฟแดง/ไฟเหลืองบนทางม้าลาย ด้วย YOLOv8 สองโมเดล ส่งข้อมูลจาก Edge
ขึ้น Cloud แบบ real-time ผ่าน MQTT → Kafka / InfluxDB → Grafana

## แผนภาพการไหลของข้อมูล (Data Flow)

```mermaid
flowchart LR
    subgraph EDGE["Edge Device (เครื่องที่รัน final.py)"]
        direction TB
        VID["วิดีโอต้นทาง\nTrain1.mp4"]
        GM["General YOLOv8 Model\n(yolov8m.pt + ByteTrack)\nตรวจจับ+ติดตาม person/car/\nmotorcycle/bus/truck"]
        TM["Traffic Light Model\n(best.pt - เทรนเอง)\nตรวจจับ RED/YELLOW/GREEN\nในกรอบ ROI"]
        LOGIC["Violation Logic\nเช็คว่ารถข้าม top_line\nและ bottom_line ครบ\nขณะไฟ RED/YELLOW"]
        PAYLOAD["สร้าง JSON Payload\ntimestamp, traffic_state,\nvehicle(id,type), pedestrian"]

        VID --> GM
        VID --> TM
        GM --> LOGIC
        TM --> LOGIC
        LOGIC -->|ฝ่าไฟจริง| PAYLOAD
    end

    subgraph SERVER["Cloud / Server (172.16.2.117)"]
        direction TB
        MQTT["VerneMQ\nMQTT Broker\ntopic: crosswalk-event-6510301037"]
        BRIDGE["mqtt_kafka_influx_bridge.py\nsubscribe MQTT แล้วแตก 2 ทาง"]
        KAFKA["Kafka\ntopic: crosswalk-event-6510301037\n(ส่งต่อ JSON ดิบ ไม่แปลงข้อมูล)"]
        INFLUX["InfluxDB\nbucket: mini_project\nmeasurement: crosswalk_6510301037\ntag: traffic_state, vehicle_type\nfield: vehicle_id, pedestrian_count,\ntraffic_state_value, vehicle_type_value"]
        GRAFANA["Grafana Dashboard\n5 panels: Total Violations,\nby Vehicle Type, by Traffic State,\nTrend over Time, Recent Log"]

        MQTT --> BRIDGE
        BRIDGE -->|raw pass-through| KAFKA
        BRIDGE -->|parse JSON เป็น Point| INFLUX
        INFLUX --> GRAFANA
    end

    PAYLOAD -->|MQTT publish\nQoS 1| MQTT
```

## คำอธิบายแต่ละขั้นตอน

| # | ขั้นตอน | รายละเอียด |
|---|---------|-----------|
| 1 | **อ่านวิดีโอ** | `final.py` เปิดไฟล์ `data/Train1.mp4` ทีละเฟรมด้วย OpenCV เมื่อจบคลิปจะวนกลับไปเฟรมแรกอัตโนมัติ (loop) |
| 2 | **ตรวจจับคน/รถ** | โมเดล YOLOv8 ทั่วไป (`yolov8m.pt`, COCO) + ByteTrack (`custom_tracker.yaml`) ตรวจจับและติดตาม ID ต่อเนื่องของ person/car/motorcycle/bus/truck |
| 3 | **ตรวจจับไฟจราจร** | โมเดลที่เทรนเอง (`runs/detect/traffic_light-2/weights/best.pt`) ตรวจจับกล่องไฟจราจรทั้งกล่อง แล้วให้สีตรงๆ จาก class (0=RED, 1=YELLOW, 2=GREEN) กรองด้วย ROI กันจับผิดจุด |
| 4 | **ตรวจการฝ่าไฟ** | เช็คว่าจุดล่างกึ่งกลางของกล่องรถ (bottom-center) ข้ามทั้ง `top_line` และ `bottom_line` ของ `crosswalk_zone` ระหว่างที่ไฟเป็น RED หรือ YELLOW หรือไม่ (จำกัดขอบเขตด้วย x-range กันรถเลนอื่นถูกนับผิด) |
| 5 | **สร้าง Payload** | เมื่อยืนยันว่าฝ่าไฟจริง (นับ 1 ครั้งต่อ track ID) สร้าง JSON payload ทันที |
| 6 | **ส่งเข้า MQTT** | Publish payload (JSON string) ไปที่ VerneMQ broker หัวข้อ `crosswalk-event-6510301037` ด้วย QoS 1 |
| 7 | **Bridge กระจายข้อมูล** | `mqtt_kafka_influx_bridge.py` subscribe topic เดียวกัน แล้วแตก 2 ทางพร้อมกัน: (ก) ส่งต่อ bytes ดิบเข้า Kafka topic เดิมทันที ไม่แปลงอะไรเลย (ข) แกะ JSON แล้วเขียนเป็น data point เข้า InfluxDB |
| 8 | **แสดงผลใน Grafana** | Grafana ต่อกับ InfluxDB โดยตรง ใช้ Flux query ดึงข้อมูลมาแสดงเป็น 5 panels (สรุปยอดรวม, แยกตามประเภทรถ, แยกตามสถานะไฟ, แนวโน้มตามเวลา, log ล่าสุด) |

## Physical / Network View

```mermaid
flowchart TB
    PC["เครื่อง Edge (Windows/Local PC)\nรัน final.py และ\nmqtt_kafka_influx_bridge.py ก็ได้\n(หรือแยกเครื่องกันก็ได้)"]
    SRV["เซิร์ฟเวอร์กลาง 172.16.2.117\n- VerneMQ : 1883\n- Kafka : 9092\n- InfluxDB : 8086"]
    GRAF["Grafana\n(เว็บ dashboard)"]

    PC -->|MQTT publish\nport 1883| SRV
    SRV -->|Flux query\nport 8086| GRAF
```
