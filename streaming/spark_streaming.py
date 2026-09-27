from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    countDistinct,
    count,
    sum,
    avg,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

from postgres_writer import (
    create_metrics_table,
    write_metrics_to_postgres,
)

from pyspark.sql.functions import approx_count_distinct

# =========================================================
# Configuration
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "fleet-telemetry"


# =========================================================
# 1. Create Spark session
# =========================================================

spark = (
    SparkSession.builder
    .appName("RideHailingFleetStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 2. Create PostgreSQL table
# =========================================================

create_metrics_table()


# =========================================================
# 3. Define incoming telemetry schema
# =========================================================

telemetry_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


# =========================================================
# 4. Read telemetry from Kafka
# =========================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# =========================================================
# 5. Convert Kafka value from binary to string
# =========================================================

json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) AS json_value"
)


# =========================================================
# 6. Parse JSON
# =========================================================

parsed_stream = (
    json_stream
    .select(
        from_json(
            col("json_value"),
            telemetry_schema
        ).alias("data")
    )
    .select("data.*")
)


# =========================================================
# 7. Convert timestamp
# =========================================================

events = (
    parsed_stream
    .withColumn(
        "event_time",
        col("timestamp").cast(TimestampType())
    )
)


# =========================================================
# 8. Create geographic zones
# =========================================================

events_with_zone = (
    events
    .withColumn(
        "zone",

        when(
            (col("lat") >= 6.90)
            & (col("lat") < 6.94)
            & (col("lon") >= 79.83)
            & (col("lon") < 79.88),

            "Colombo-Central"
        )

        .when(
            (col("lat") >= 6.90)
            & (col("lat") < 6.98)
            & (col("lon") >= 79.88)
            & (col("lon") < 79.95),

            "Colombo-East"
        )

        .when(
            (col("lat") >= 6.90)
            & (col("lat") < 7.05)
            & (col("lon") >= 79.80)
            & (col("lon") < 79.83),

            "Colombo-West"
        )

        .otherwise("Other")
    )
)


# =========================================================
# 9. Real-time fleet metrics
# =========================================================

metrics = (
    events_with_zone

    .withWatermark(
        "event_time",
        "2 minutes"
    )

    .groupBy(
        window(
            col("event_time"),
            "5 minutes",
            "1 minute"
        )
    )

    .agg(

        approx_count_distinct(
            "vehicle_id"
        ).alias(
            "active_vehicles"
        ),

        count(
            when(
                col("status") == "idle",
                True
            )
        ).alias(
            "idle_events"
        ),

        count("*").alias(
            "total_events"
        ),

        approx_count_distinct(
            "trip_id"
        ).alias(
            "trips"
        ),

        sum(
            "fare"
        ).alias(
            "total_earnings"
        ),

        avg(
            "speed"
        ).alias(
            "average_speed"
        )
    )

    .withColumn(
        "idle_ratio",
        col("idle_events")
        / col("total_events")
    )
)


# =========================================================
# 10. Write to PostgreSQL using foreachBatch
# =========================================================

postgres_query = (
    metrics
    .writeStream
    .outputMode("update")
    .foreachBatch(
        write_metrics_to_postgres
    )
    .option(
        "checkpointLocation",
        "checkpoints/fleet_metrics_postgres"
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)


# =========================================================
# 11. Keep streaming application running
# =========================================================

postgres_query.awaitTermination()