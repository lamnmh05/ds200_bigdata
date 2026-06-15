from __future__ import annotations

from src.config import settings
from src.messaging.kafka_client import create_consumer, iter_json_messages
from src.storage.sqlite_store import DetectionStore


def main() -> None:
    consumer = create_consumer("lab5-storage-server", [settings.detection_topic])
    store = DetectionStore(settings.output_db, settings.output_jsonl)

    print(f"Storage server started. Reading detections from '{settings.detection_topic}'.")
    try:
        for detection_message in iter_json_messages(consumer):
            store.save(detection_message)
            print(
                f"Saved frame_id={detection_message['frame_id']} "
                f"person_count={detection_message['person_count']}"
            )
    except KeyboardInterrupt:
        print("Storage server interrupted by user.")
    finally:
        store.close()
        consumer.close()


if __name__ == "__main__":
    main()
