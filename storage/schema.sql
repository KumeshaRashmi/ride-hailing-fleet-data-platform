CREATE TABLE IF NOT EXISTS fleet_utilization_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    zone TEXT NOT NULL,
    telemetry_event_count INTEGER NOT NULL,
    idle_event_count INTEGER NOT NULL,
    on_trip_event_count INTEGER NOT NULL,
    active_event_count INTEGER NOT NULL,
    observed_earnings NUMERIC(14, 2) NOT NULL,
    average_speed NUMERIC(8, 2),
    idle_ratio NUMERIC(7, 4) NOT NULL,
    spark_batch_id BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, zone)
);

CREATE INDEX IF NOT EXISTS idx_utilization_metrics_processed_at
    ON fleet_utilization_metrics (processed_at DESC);

CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    report_date DATE NOT NULL,
    vehicle_id TEXT NOT NULL,
    completed_trip_count INTEGER NOT NULL,
    trip_revenue NUMERIC(14, 2) NOT NULL,
    fuel_cost NUMERIC(14, 2) NOT NULL,
    maintenance_cost NUMERIC(14, 2) NOT NULL,
    total_expense NUMERIC(14, 2) NOT NULL,
    distance_covered NUMERIC(12, 2) NOT NULL,
    service_flag TEXT NOT NULL,
    profit NUMERIC(14, 2) NOT NULL,
    profit_margin_pct NUMERIC(8, 2),
    profitability_status TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_profitability_date_status
    ON daily_vehicle_profitability (report_date, profitability_status);

CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    severity TEXT NOT NULL,
    rule_name TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb
);
