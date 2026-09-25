import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from data_generators.streaming_generator import generate_event


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "fleet-telemetry"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5
    )


def main():
    print("Connecting to Kafka...")

    producer = create_producer()

    print("Connected to Kafka.")
    print(f"Publishing to topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = generate_event()

            future = producer.send(
                KAFKA_TOPIC,
                value=event
            )

            try:
                metadata = future.get(timeout=10)

                print(
                    f"Sent event | "
                    f"vehicle={event['vehicle_id']} | "
                    f"status={event['status']} | "
                    f"partition={metadata.partition} | "
                    f"offset={metadata.offset}"
                )

            except KafkaError as error:
                print(f"Kafka send error: {error}")

            time.sleep(3)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()