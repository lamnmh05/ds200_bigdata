from __future__ import annotations

import argparse
import time
from pathlib import Path

import cv2

from src.config import BASE_DIR
from src.config import settings
from src.messaging.kafka_client import create_producer, send_json
from src.utils.image_codec import encode_frame
from src.utils.schemas import build_frame_message


def parse_source(value: str) -> int | str:
    if value.isdigit():
        return int(value)

    source_path = Path(value)
    if source_path.exists():
        return str(source_path)

    lab_relative_path = BASE_DIR / source_path
    if lab_relative_path.exists():
        return str(lab_relative_path)

    return str(source_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read camera frames and publish them to Kafka.")
    parser.add_argument(
        "--source",
        default="0",
        help="Camera index, for example 0, or a video path such as data/sample.mp4.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = parse_source(args.source)
    capture = cv2.VideoCapture(source)
    if not capture.isOpened():
        raise RuntimeError(
            "Could not open video source. "
            f"Input='{args.source}', resolved='{source}'. "
            "Use --source 0 for webcam, or pass a valid path such as data/sample.mp4."
        )

    producer = create_producer()
    frame_id = 0
    delay = 1.0 / settings.camera_fps if settings.camera_fps > 0 else 0

    print(f"Camera server started. Sending frames to topic '{settings.frame_topic}'.")
    try:
        while True:
            started_at = time.time()
            success, frame = capture.read()
            if not success:
                print("No more frames from source. Camera server stopped.")
                break

            height, width = frame.shape[:2]
            encoded_image = encode_frame(frame, settings.jpeg_quality)
            payload = build_frame_message(frame_id, width, height, encoded_image)
            send_json(producer, settings.frame_topic, payload)
            producer.flush(1)

            print(f"Sent frame_id={frame_id} size={width}x{height}")
            frame_id += 1

            elapsed = time.time() - started_at
            if delay > elapsed:
                time.sleep(delay - elapsed)
    except KeyboardInterrupt:
        print("Camera server interrupted by user.")
    finally:
        producer.flush(5)
        capture.release()


if __name__ == "__main__":
    main()
