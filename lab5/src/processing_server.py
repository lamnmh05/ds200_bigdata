from __future__ import annotations

from src.config import settings
from src.detection.yolo_detector import YoloPersonDetector
from src.messaging.kafka_client import create_consumer, create_producer, iter_json_messages, send_json
from src.utils.image_codec import decode_frame
from src.utils.schemas import build_detection_message


def main() -> None:
    consumer = create_consumer("lab5-processing-server", [settings.frame_topic])
    producer = create_producer()
    detector = YoloPersonDetector()

    print(
        "Processing server started. "
        f"Reading '{settings.frame_topic}' and writing '{settings.detection_topic}'."
    )
    try:
        for frame_message in iter_json_messages(consumer):
            frame = decode_frame(frame_message["image"])
            boxes = detector.detect_people(frame)
            detection_message = build_detection_message(frame_message, boxes)
            send_json(producer, settings.detection_topic, detection_message)
            producer.flush(1)

            print(
                f"Processed frame_id={detection_message['frame_id']} "
                f"person_count={detection_message['person_count']}"
            )
    except KeyboardInterrupt:
        print("Processing server interrupted by user.")
    finally:
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
