import os

import psycopg2


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "fleetdb"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def create_metrics_table():
    connection = get_postgres_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fleet_realtime_metrics (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            active_vehicles INTEGER,
            idle_events INTEGER,
            total_events INTEGER,
            trips INTEGER,
            total_earnings DOUBLE PRECISION,
            average_speed DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    connection.commit()

    cursor.close()
    connection.close()

    print("PostgreSQL table ready: fleet_realtime_metrics")


def write_metrics_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    connection = get_postgres_connection()
    cursor = connection.cursor()

    try:
        rows = batch_df.collect()

        for row in rows:

            window_start = row["window"]["start"]
            window_end = row["window"]["end"]

            cursor.execute(
                """
                INSERT INTO fleet_realtime_metrics (
                    window_start,
                    window_end,
                    active_vehicles,
                    idle_events,
                    total_events,
                    trips,
                    total_earnings,
                    average_speed
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    window_start,
                    window_end,
                    row["active_vehicles"],
                    row["idle_events"],
                    row["total_events"],
                    row["trips"],
                    row["total_earnings"],
                    row["average_speed"],
                ),
            )

        connection.commit()

        print(
            f"Batch {batch_id}: "
            f"{len(rows)} metric rows written to PostgreSQL"
        )

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()