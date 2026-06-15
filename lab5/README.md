# Lab 5: Đếm số người với Kafka và YOLO

Hệ thống mô phỏng pipeline xử lý dữ liệu lớn dạng streaming:

- **Camera server** đọc frame từ webcam/video và gửi vào Kafka topic `camera.frames`.
- **Processing server** nhận frame, chạy YOLO đếm người và tạo bounding box.
- **Storage server** nhận kết quả, lưu vào SQLite và JSONL.

Kafka đóng vai trò message broker giữa các server. Demo chạy trên Windows, laptop CPU.

## Cấu trúc thư mục

```text
lab5/
+-- docker-compose.yml
+-- requirements.txt
+-- run_all_videos.ps1
+-- data/
    +-- vid_1.mp4
+-- outputs/
    +-- annotated_vid_1.mp4
    +-- detections.jsonl
+-- src/
    +-- config.py
    +-- camera_server.py
    +-- processing_server.py
    +-- storage_server.py
    +-- render_video.py
```

## Cài đặt

- Python 3.11.
- UV.
- Docker Desktop.
- Các thư viện trong `requirements.txt`.
- YOLO model `yolov8n.pt`.

## Chạy sample mẫu

Bật Kafka:

```powershell
docker compose up -d
```

Terminal 1:

```powershell
python -m src.storage_server
```

Terminal 2:

```powershell
python -m src.processing_server
```

Terminal 3:

```powershell
python -m src.camera_server --source data/vid_1.mp4
```

```powershell
python -m src.render_video --source data/vid_1.mp4 --output outputs/annotated_vid_1.mp4
```

## Chạy bằng webcam

```powershell
python -m src.camera_server --source 0
```

## Dừng Kafka

```powershell
docker compose down
```

## File kết quả

`outputs/detections.jsonl` chứa mỗi dòng là một frame đã xử lý:

```json
{
  "frame_id": 0,
  "timestamp": 1781496825.17,
  "processed_at": 1781496831.41,
  "person_count": 2,
  "boxes": [
    {
      "x1": 10.5,
      "y1": 30.2,
      "x2": 120.8,
      "y2": 240.6,
      "confidence": 0.87,
      "class_id": 0,
      "label": "person"
    }
  ]
}
```