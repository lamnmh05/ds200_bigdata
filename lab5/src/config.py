from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")


def _path_from_env(name: str, default: str) -> Path:
    value = os.getenv(name, default)
    path = Path(value)
    return path if path.is_absolute() else BASE_DIR / path


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    frame_topic: str = os.getenv("FRAME_TOPIC", "camera.frames")
    detection_topic: str = os.getenv("DETECTION_TOPIC", "camera.detections")
    model_path: Path = _path_from_env("MODEL_PATH", "models/yolov8n.pt")
    output_db: Path = _path_from_env("OUTPUT_DB", "outputs/detections.db")
    output_jsonl: Path = _path_from_env("OUTPUT_JSONL", "outputs/detections.jsonl")
    camera_fps: float = float(os.getenv("CAMERA_FPS", "5"))
    jpeg_quality: int = int(os.getenv("JPEG_QUALITY", "80"))
    confidence_threshold: float = float(os.getenv("CONFIDENCE_THRESHOLD", "0.35"))
    kafka_poll_timeout: float = float(os.getenv("KAFKA_POLL_TIMEOUT", "1.0"))


settings = Settings()
