from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class DetectionStore:
    def __init__(self, db_path: Path, jsonl_path: Path) -> None:
        self.db_path = db_path
        self.jsonl_path = jsonl_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self._create_table()

    def _create_table(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS detections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                frame_id INTEGER NOT NULL,
                captured_at REAL NOT NULL,
                processed_at REAL NOT NULL,
                person_count INTEGER NOT NULL,
                boxes_json TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def save(self, detection: dict[str, Any]) -> None:
        boxes_json = json.dumps(detection["boxes"], ensure_ascii=False)
        self.connection.execute(
            """
            INSERT INTO detections (
                frame_id,
                captured_at,
                processed_at,
                person_count,
                boxes_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(detection["frame_id"]),
                float(detection["timestamp"]),
                float(detection["processed_at"]),
                int(detection["person_count"]),
                boxes_json,
            ),
        )
        self.connection.commit()

        with self.jsonl_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(detection, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self.connection.close()
