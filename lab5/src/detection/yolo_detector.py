from __future__ import annotations

from pathlib import Path

import numpy as np
from ultralytics import YOLO

from src.config import settings
from src.utils.schemas import BoundingBox


PERSON_CLASS_ID = 0


class YoloPersonDetector:
    def __init__(
        self,
        model_path: Path = settings.model_path,
        confidence_threshold: float = settings.confidence_threshold,
    ) -> None:
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_source = str(model_path) if model_path.exists() else model_path.name
        self.model = YOLO(model_source)

    def detect_people(self, frame: np.ndarray) -> list[BoundingBox]:
        results = self.model.predict(
            source=frame,
            conf=self.confidence_threshold,
            classes=[PERSON_CLASS_ID],
            device="cpu",
            verbose=False,
        )

        boxes: list[BoundingBox] = []
        for result in results:
            for detected_box in result.boxes:
                x1, y1, x2, y2 = detected_box.xyxy[0].tolist()
                confidence = float(detected_box.conf[0])
                class_id = int(detected_box.cls[0])
                boxes.append(
                    {
                        "x1": float(x1),
                        "y1": float(y1),
                        "x2": float(x2),
                        "y2": float(y2),
                        "confidence": confidence,
                        "class_id": class_id,
                        "label": "person",
                    }
                )
        return boxes
