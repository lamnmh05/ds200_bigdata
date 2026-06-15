from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from src.config import settings


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def create_consumer(group_id: str, topics: list[str]) -> KafkaConsumer:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def send_json(producer: KafkaProducer, topic: str, payload: dict[str, Any]) -> None:
    producer.send(topic, payload)


def iter_json_messages(consumer: KafkaConsumer) -> Iterator[dict[str, Any]]:
    while True:
        records = consumer.poll(timeout_ms=int(settings.kafka_poll_timeout * 1000))
        for messages in records.values():
            for message in messages:
                yield message.value
