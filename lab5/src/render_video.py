from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import cv2

from src.config import BASE_DIR


def resolve_path(value: str) -> Path:
    path = Path(value)
    if path.exists():
        return path

    lab_relative_path = BASE_DIR / path
    if lab_relative_path.exists():
        return lab_relative_path

    return path


def load_detections(path: Path) -> dict[int, dict[str, Any]]:
    detections: dict[int, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            detections[int(item["frame_id"])] = item
    return detections


def draw_detection(frame, detection: dict[str, Any] | None) -> None:
    if detection is None:
        cv2.putText(
            frame,
            "person_count: N/A",
            (20, 40),
            cv2.FONT_HERSHEY_SIMPLEX,
            1.0,
            (0, 255, 255),
            2,
            cv2.LINE_AA,
        )
        return

    person_count = int(detection["person_count"])
    cv2.putText(
        frame,
        f"person_count: {person_count}",
        (20, 40),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.0,
        (0, 255, 0),
        2,
        cv2.LINE_AA,
    )

    for box in detection["boxes"]:
        x1 = int(box["x1"])
        y1 = int(box["y1"])
        x2 = int(box["x2"])
        y2 = int(box["y2"])
        confidence = float(box["confidence"])

        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
        cv2.putText(
            frame,
            f"person {confidence:.2f}",
            (x1, max(20, y1 - 10)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 255, 0),
            2,
            cv2.LINE_AA,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render an annotated video from detection JSONL.")
    parser.add_argument("--source", required=True, help="Input video path, for example data/sample.mp4.")
    parser.add_argument(
        "--detections",
        default="outputs/detections.jsonl",
        help="Detection JSONL path.",
    )
    parser.add_argument(
        "--output",
        default="outputs/annotated.mp4",
        help="Output annotated video path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_path = resolve_path(args.source)
    detections_path = resolve_path(args.detections)
    output_path = BASE_DIR / args.output if not Path(args.output).is_absolute() else Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not source_path.exists():
        raise FileNotFoundError(f"Input video not found: {source_path}")
    if not detections_path.exists():
        raise FileNotFoundError(f"Detections file not found: {detections_path}")

    detections = load_detections(detections_path)
    capture = cv2.VideoCapture(str(source_path))
    if not capture.isOpened():
        raise RuntimeError(f"Could not open input video: {source_path}")

    fps = capture.get(cv2.CAP_PROP_FPS) or 25
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    writer = cv2.VideoWriter(
        str(output_path),
        cv2.VideoWriter_fourcc(*"mp4v"),
        fps,
        (width, height),
    )

    frame_id = 0
    written_frames = 0
    try:
        while True:
            success, frame = capture.read()
            if not success:
                break

            draw_detection(frame, detections.get(frame_id))
            writer.write(frame)
            written_frames += 1
            frame_id += 1
    finally:
        capture.release()
        writer.release()

    print(f"Annotated video saved to: {output_path}")
    print(f"Frames written: {written_frames}")


if __name__ == "__main__":
    main()
