import json
import random
import time
from datetime import datetime, timezone


VEHICLES = [f"V{i:03d}" for i in range(1, 21)]
DRIVERS = [f"D{i:03d}" for i in range(1, 21)]

STATUSES = [
    "idle",
    "enroute",
    "on_trip"
]


def generate_event():
    """
    Generate one simulated ride-hailing telemetry event.
    """

    status = random.choice(STATUSES)

    # Idle vehicles normally have zero/very low speed.
    if status == "idle":
        speed = random.randint(0, 3)
        fare = 0.0

    elif status == "enroute":
        speed = random.randint(10, 60)
        fare = 0.0

    else:
        speed = random.randint(10, 80)
        fare = round(random.uniform(300, 3000), 2)

    event = {
        "trip_id": f"T{random.randint(10000, 99999)}",
        "driver_id": random.choice(DRIVERS),
        "vehicle_id": random.choice(VEHICLES),

        # Simulated Colombo-area coordinates
        "lat": round(random.uniform(6.8500, 7.0500), 6),
        "lon": round(random.uniform(79.8000, 79.9500), 6),

        "speed": speed,
        "status": status,
        "fare": fare,

        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return event


def main():
    print("Starting fleet telemetry generator...")
    print("Press Ctrl+C to stop.")

    while True:
        event = generate_event()

        print(json.dumps(event))

        time.sleep(3)


if __name__ == "__main__":
    main()