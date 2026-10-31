-- Cloudflare D1 Schema for Distributed Systems Edge Lab

-- 1. Saga Pattern Transaction Coordinator Tables
CREATE TABLE IF NOT EXISTS inventory_stock (
    sku TEXT PRIMARY KEY,
    initial_qty INTEGER NOT NULL,
    available_qty INTEGER NOT NULL,
    reserved_qty INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO inventory_stock (sku, initial_qty, available_qty, reserved_qty) VALUES
    ('SKU-LAPTOP', 10000, 10000, 0),
    ('SKU-PHONE', 10000, 10000, 0),
    ('SKU-MONITOR', 10000, 10000, 0);

CREATE TABLE IF NOT EXISTS sagas (
    saga_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    version_epoch INTEGER NOT NULL DEFAULT 1,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    failed_step TEXT,
    error TEXT,
    detect_latency_us REAL DEFAULT 0,
    rollback_latency_us REAL DEFAULT 0,
    zombie_fenced INTEGER DEFAULT 0,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS saga_step_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    saga_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    timestamp_ms REAL NOT NULL,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS service_ledger (
    saga_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    inv_status TEXT NOT NULL DEFAULT 'NONE',
    pay_status TEXT NOT NULL DEFAULT 'NONE',
    ship_status TEXT NOT NULL DEFAULT 'NONE'
);

CREATE TABLE IF NOT EXISTS service_tombstones (
    saga_id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    fenced_hits INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS circuit_breakers (
    service_name TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'CLOSED',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    fast_rejects INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO circuit_breakers (service_name, state, consecutive_failures, fast_rejects, updated_at_ms) VALUES
    ('reserve_inventory', 'CLOSED', 0, 0, 0),
    ('charge_payment', 'CLOSED', 0, 0, 0),
    ('create_shipment', 'CLOSED', 0, 0, 0);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    saga_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    resolved INTEGER NOT NULL DEFAULT 0
);

-- 2. Streaming vs. Batch Reconciliation Engine Tables
CREATE TABLE IF NOT EXISTS raw_events_batch (
    event_id TEXT PRIMARY KEY,
    window_id INTEGER NOT NULL,
    user_id TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    event_time_sec REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_events_window ON raw_events_batch(window_id);

CREATE TABLE IF NOT EXISTS streaming_windows (
    window_id INTEGER PRIMARY KEY,
    event_count INTEGER NOT NULL DEFAULT 0,
    amount_sum_cents INTEGER NOT NULL DEFAULT 0,
    hll_registers_b64 TEXT NOT NULL,
    exact_users_override INTEGER,
    dropped_injected INTEGER NOT NULL DEFAULT 0,
    dup_injected INTEGER NOT NULL DEFAULT 0,
    late_injected INTEGER NOT NULL DEFAULT 0,
    diagnosed_anomaly TEXT NOT NULL DEFAULT 'NONE',
    auto_corrected INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at_ms INTEGER NOT NULL,
    reconcile_latency_ms REAL NOT NULL,
    total_windows INTEGER NOT NULL,
    flagged_windows INTEGER NOT NULL,
    mean_drift_before_pct REAL NOT NULL,
    mean_drift_after_pct REAL NOT NULL,
    root_cause_accuracy_pct REAL NOT NULL,
    details_json TEXT NOT NULL
);

-- 3. Distributed Go MapReduce Cluster Tables
CREATE TABLE IF NOT EXISTS mapreduce_jobs (
    job_id TEXT PRIMARY KEY,
    scenario TEXT NOT NULL,
    records INTEGER NOT NULL,
    chunks INTEGER NOT NULL,
    workers INTEGER NOT NULL,
    wall_time_ms REAL NOT NULL,
    throughput_rec_sec REAL NOT NULL,
    speedup REAL NOT NULL,
    failures_recovered INTEGER NOT NULL,
    speculative_wins INTEGER NOT NULL,
    data_loss_pct REAL NOT NULL,
    mode TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
