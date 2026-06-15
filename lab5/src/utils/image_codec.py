from __future__ import annotations

import base64

import cv2
import numpy as np


def encode_frame(frame: np.ndarray, jpeg_quality: int) -> str:
    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), int(jpeg_quality)]
    success, buffer = cv2.imencode(".jpg", frame, encode_params)
    if not success:
        raise ValueError("Could not encode frame as JPEG")
    return base64.b64encode(buffer).decode("ascii")


def decode_frame(encoded_frame: str) -> np.ndarray:
    raw = base64.b64decode(encoded_frame.encode("ascii"))
    array = np.frombuffer(raw, dtype=np.uint8)
    frame = cv2.imdecode(array, cv2.IMREAD_COLOR)
    if frame is None:
        raise ValueError("Could not decode JPEG frame")
    return frame
