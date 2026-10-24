-- . Pipeline execution history
CREATE TABLE IF NOT EXISTS pipeline_runs (
run_id BIGSERIAL PRIMARY KEY,
pipeline_name VARCHAR(100) NOT NULL,
started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
completed_at TIMESTAMPTZ,
status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
sla_deadline TIMESTAMPTZ,
error_message TEXT,
CONSTRAINT valid_pipeline_status
CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SLA_BREACHED'))
);

-- 2. Data freshness and SLA monitoring metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
id BIGSERIAL PRIMARY KEY,
dataset_name VARCHAR(100) NOT NULL,
latest_data_timestamp TIMESTAMPTZ,
checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
freshness_minutes NUMERIC(12, 2),
latency_seconds NUMERIC(12, 2),
status VARCHAR(30) NOT NULL,
CONSTRAINT valid_metric_status
CHECK (status IN (
'HEALTHY',
'STALE',
'NO_DATA',
'SLA_BREACHED',
'ERROR'
))
);

-- 4. Indexes for faster monitoring queries
CREATE INDEX IF NOT EXISTS idx_transactions_event_timestamp
ON transactions (event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_ingested_at
ON transactions (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_started
ON pipeline_runs (pipeline_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_dataset_checked
ON pipeline_metrics (dataset_name, checked_at DESC);