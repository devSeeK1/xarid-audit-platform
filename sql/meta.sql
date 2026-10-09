-- Meta (texnik) jadvallar: ishga tushirishlar jurnali, lineage, sifat natijalari, watermark
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS meta.etl_runs (
    run_id        VARCHAR PRIMARY KEY,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    status        VARCHAR,          -- running | success | failed
    run_by        VARCHAR,          -- kim ishga tushirgan (OS foydalanuvchisi)
    host          VARCHAR,
    mode          VARCHAR,          -- incremental | full
    duration_sec  DOUBLE,
    error         VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.source_lineage (
    run_id        VARCHAR,
    source        VARCHAR,
    location      VARCHAR,
    sha256        VARCHAR,
    rows          BIGINT,
    extracted_at  VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.dq_results (
    run_id        VARCHAR,
    dataset       VARCHAR,
    check_name    VARCHAR,
    failed_rows   BIGINT
);

CREATE TABLE IF NOT EXISTS meta.dataset_stats (
    run_id        VARCHAR,
    dataset       VARCHAR,
    rows_in       BIGINT,
    rows_out      BIGINT,
    rows_quarantined BIGINT
);

CREATE TABLE IF NOT EXISTS meta.watermarks (
    source        VARCHAR PRIMARY KEY,
    last_value    DATE,
    updated_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meta.step_timings (
    run_id        VARCHAR,
    step          VARCHAR,
    seconds       DOUBLE
);
