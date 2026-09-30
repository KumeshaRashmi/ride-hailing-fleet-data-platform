import json
import os
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from data_generators.streaming_generator import generate_event
from observability.logging import get_logger, log_event


KAFKA_BOOTSTRAP_SERVERS = os.getenv("FLEET_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("FLEET_KAFKA_TOPIC", "fleet-telemetry")
EVENT_INTERVAL_SECONDS = float(os.getenv("FLEET_EVENT_INTERVAL_SECONDS", "3"))
LOGGER = get_logger("fleet.kafka_producer")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        acks="all",
    )


def main() -> None:
    if EVENT_INTERVAL_SECONDS <= 0:
        raise ValueError("FLEET_EVENT_INTERVAL_SECONDS must be positive")

    log_event(
        LOGGER,
        "producer_connecting",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
    )

    producer = create_producer()
    log_event(LOGGER, "producer_connected", topic=KAFKA_TOPIC)

    try:
        while True:
            event = generate_event()

            future = producer.send(
                KAFKA_TOPIC,
                value=event
            )

            try:
                metadata = future.get(timeout=10)
                log_event(
                    LOGGER,
                    "telemetry_event_published",
                    topic=KAFKA_TOPIC,
                    vehicle_id=event["vehicle_id"],
                    driver_id=event["driver_id"],
                    trip_id=event["trip_id"],
                    status=event["status"],
                    partition=metadata.partition,
                    offset=metadata.offset,
                )

            except KafkaError as error:
                log_event(
                    LOGGER,
                    "telemetry_publish_failed",
                    level=40,
                    topic=KAFKA_TOPIC,
                    error=str(error),
                )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        log_event(LOGGER, "producer_stopping")

    finally:
        producer.flush()
        producer.close()
        log_event(LOGGER, "producer_stopped")


if __name__ == "__main__":
    main()
