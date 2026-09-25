from data_generators.streaming_generator import generate_event


def test_event_contains_required_fields():
    event = generate_event()

    required_fields = {
        "trip_id",
        "driver_id",
        "vehicle_id",
        "lat",
        "lon",
        "speed",
        "status",
        "fare",
        "timestamp"
    }

    assert required_fields.issubset(event.keys())


def test_status_is_valid():
    event = generate_event()

    assert event["status"] in {
        "idle",
        "enroute",
        "on_trip"
    }


def test_vehicle_id_format():
    event = generate_event()

    assert event["vehicle_id"].startswith("V")