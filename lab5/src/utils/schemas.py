from __future__ import annotations

import time
from typing import Any, TypedDict


class FrameMessage(TypedDict):
    frame_id: int
    timestamp: float
    width: int
    height: int
    image: str


class BoundingBox(TypedDict):
    x1: float
    y1: float
    x2: float
    y2: float
    confidence: float
    class_id: int
    label: str


class DetectionMessage(TypedDict):
    frame_id: int
    timestamp: float
    processed_at: float
    person_count: int
    boxes: list[BoundingBox]


def build_frame_message(frame_id: int, width: int, height: int, image: str) -> FrameMessage:
    return {
        "frame_id": frame_id,
        "timestamp": time.time(),
        "width": width,
        "height": height,
        "image": image,
    }


def build_detection_message(frame: dict[str, Any], boxes: list[BoundingBox]) -> DetectionMessage:
    return {
        "frame_id": int(frame["frame_id"]),
        "timestamp": float(frame["timestamp"]),
        "processed_at": time.time(),
        "person_count": len(boxes),
        "boxes": boxes,
    }
