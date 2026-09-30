# Architecture Decision Record — Ward Vital Signs Monitoring Pipeline

| Field    | Value                                              |
|----------|----------------------------------------------------|
| Status   | Accepted                                           |
| Date     | 2026-09-30                                         |
| Decision | **Kappa architecture** (single streaming path, Kafka as the replayable source of truth) |
| Rejected | Lambda architecture (separate batch + speed layers) |

> **Disclaimer.** All data in this project is synthetic. The risk score is a simplified,
> NEWS2-*style* illustration built for a data-engineering exercise. It is **not** a clinical
> tool and must not be used for any medical decision.

---

## 1. Context

A hospital ward needs:

1. **Near-real-time** visibility of patient vitals from bedside monitors
   (`patient_id, heart_rate, spo2, systolic_bp, diastolic_bp, temperature, timestamp`),
   emitted every few seconds per patient.
2. **Once-a-day** lab results from pathology
   (`patient_id, test_type, result_value, reference_range, collected_at`), one file per day.
3. Answers to: *which patients show concerning vital-sign trends right now*, and *how do
   yesterday's labs change the risk picture going forward*.

Outputs: real-time API figures, threshold-based per-patient alerts, and a daily consolidated
risk report joining vitals trends with the latest labs.

### Workload profile (drives every decision below)

| Property | Vitals | Labs |
|---|---|---|
| Arrival | Continuous, unbounded | One file per simulated day |
| Volume (default config) | 20 patients × 1 reading/real-second ≈ 20 events/s | ~20 patients × ~7 tests ≈ 150 rows/day |
| Latency need | Seconds (alerts) | Minutes (next report / next risk update) |
| Ordering need | Per patient (trend/slope detection) | Per patient, latest-wins |
| Correction/replay need | Yes — fix scoring logic and recompute | Yes — re-load a bad file idempotently |

The key observation: **the only genuinely "batch" input is tiny** (hundreds of rows per day).
Nothing in this use case needs a heavyweight batch recomputation over a huge master dataset.

---

## 2. Options considered

### Option A — Lambda

- **Batch layer:** raw vitals + labs land in an immutable master store (Parquet); a nightly
  Spark batch job recomputes accurate daily views (trends, scores, joins).
- **Speed layer:** Spark Structured Streaming computes approximate real-time views for
  alerts and the API.
- **Serving layer:** merges batch views (authoritative, older) with speed views (recent).

### Option B — Kappa (recommended)

- **One log:** every input — vitals *and* lab rows — enters Kafka. Kafka (with adequate
  retention) is the replayable source of truth.
- **One processing path:** a single Spark Structured Streaming application validates, windows,
  scores, joins with labs and writes idempotently to PostgreSQL.
- **Reprocessing = replay:** to fix logic, deploy the new job with a fresh checkpoint and
  consumer position `earliest`, let it rebuild the tables (or new versioned tables), then switch.
- **Airflow's role** is *ingestion adapter and report scheduler*, not a second computation
  layer: it senses the daily lab file, validates it, publishes the rows into Kafka, and later
  renders the daily report from tables the stream job already maintains.

---

## 3. Comparison against this use case

| Criterion | Lambda | Kappa | Winner for *this* ward |
|---|---|---|---|
| **Latency** | Speed layer gives seconds; batch views lag up to 1 sim day | Seconds for everything, including lab effects (labs feed the live score as soon as they are published) | **Kappa** — "how do labs change the risk picture *going forward*" is answered immediately, not after the next batch run |
| **Replay / reprocessing** | Recompute from master Parquet with the batch code | Replay Kafka topics from `earliest` through the *same* streaming code | **Kappa** — our retention (see §6) easily covers the whole demo history; the batch input is small enough to keep in Kafka indefinitely |
| **Consistency** | Two implementations of scoring/trend logic (batch + streaming) must be kept identical; divergence produces different numbers in the API vs report — a real safety concern on a ward | One implementation; the API, alerts and daily report all read tables produced by the same code | **Kappa** — single source of logic is the strongest argument |
| **Cost / resources** | Streaming job + batch Spark jobs + merge logic; more RAM on a laptop, more code to test | One Spark app (local mode, 3 queries), Airflow only runs lightweight Python tasks | **Kappa** — fits comfortably in a laptop's Docker memory budget |
| **Operational complexity** | Two pipelines to monitor, two failure modes, merge semantics in serving | One pipeline; checkpoints + idempotent sinks give exactly-once *effects* | **Kappa** |
| **Where Lambda would win** | Very large historical recompute (years of data), heavy ML retraining, data too big to keep in the log | Weak if log retention is short or reprocessing must scan terabytes | Not our situation |

### Decision

**Adopt Kappa.** The deciding factors are **consistency** (one scoring implementation shared by
alerts, API and report) and **latency of lab impact** (labs update live risk the moment they
are published). Replay is realistic because the data volume is small and Kafka retention is
configured to cover the full simulated history.

### Why Lambda is rejected (for the viva)

1. The "batch" data is ~150 rows/day — a batch *layer* would be infrastructure without a workload.
2. Duplicating NEWS2-style scoring and trend logic in two code paths risks the API and the daily
   report disagreeing about the same patient. In a clinical-monitoring context, contradictory
   numbers are worse than slightly late numbers.
3. It would roughly double the Spark footprint on a single laptop.

### Honest trade-offs we accept

- **Replay time grows with history, and history is bounded by retention.** Mitigation: vitals
  retention (7 real days ≈ 2,000 sim days) far exceeds any demo run. Long-term raw archival
  (e.g. Kafka → Parquet/object storage) is deliberately out of scope for the laptop build and
  listed as a production improvement.
- **Kafka becomes critical state.** Mitigation: single broker is acceptable for a laptop demo;
  production would use RF=3, `min.insync.replicas=2` (documented in report notes).
- **Stream–table join semantics.** Labs are joined per micro-batch against the latest lab
  snapshot, so a vitals window computed *before* a lab arrives is not retro-scored. This is the
  intended semantics ("going forward"), and the daily report re-joins the full day explicitly.

---

## 4. Architecture diagram

```mermaid
flowchart LR
    subgraph Sources["Sources (Python simulators)"]
        VS["Vitals simulator<br/>per-patient baselines,<br/>deterioration, spikes,<br/>malformed + late events"]
        LS["Lab file simulator<br/>1 CSV per simulated day<br/>→ data/landing/"]
    end

    subgraph Airflow["Apache Airflow"]
        D1["lab_ingest DAG<br/>FileSensor → validate (DQ)<br/>→ publish to Kafka → archive"]
        D2["daily_risk_report DAG<br/>wait for labs loaded →<br/>join trends + labs →<br/>HTML/CSV report"]
    end

    subgraph Kafka["Apache Kafka (KRaft, 1 broker)"]
        T1[("vitals.raw<br/>key=patient_id<br/>6 partitions")]
        T2[("labs.raw<br/>key=patient_id<br/>3 partitions")]
        T3[("alerts.patient<br/>key=patient_id")]
        T4[("deadletter<br/>key=source")]
    end

    subgraph Spark["Spark Structured Streaming (1 app, local mode)"]
        Q1["Q1 vitals: parse → validate →<br/>watermark → tumbling + sliding windows →<br/>slope/trend → EWS score →<br/>join latest labs → alerts"]
        Q2["Q2 labs: parse → validate →<br/>flag abnormal → upsert"]
        CK[("checkpoints/")]
    end

    subgraph Storage
        PG[("PostgreSQL<br/>db: ward")]
    end

    API["FastAPI<br/>/ward/summary · /patients/{id}<br/>/alerts · /reports/latest<br/>/health · /metrics"]
    REP["reports/<br/>risk_report_YYYY-MM-DD<br/>.html / .csv"]

    subgraph Obs["Observability"]
        PR["Prometheus<br/>+ alert rules"]
        GR["Grafana<br/>provisioned dashboard"]
    end

    VS -->|produce| T1
    LS -->|drop file| D1
    D1 -->|valid rows| T2
    D1 -->|bad rows| T4
    T1 --> Q1
    T2 --> Q2
    Q1 -->|invalid / too late| T4
    Q1 -->|foreachBatch upsert| PG
    Q1 --> T3
    Q2 -->|foreachBatch upsert| PG
    Q1 --- CK
    Q2 --- CK
    PG --> D2 --> REP
    PG --> API
    REP --> API

    VS -. /metrics .-> PR
    Q1 -. /metrics .-> PR
    API -. "/metrics (incl. batch/DQ<br/>gauges read from PG)" .-> PR
    D1 -. "lab_file_loads,<br/>pipeline_runs" .-> PG
    D2 -. pipeline_runs .-> PG
    PR --> GR
    PG -. SQL datasource .-> GR
```

---

## 5. Simulated clock

All components share one definition of "simulated time" (implemented once in the shared
`common/` package and imported everywhere).

| Setting (`.env`) | Default | Meaning |
|---|---|---|
| `SIM_DAY_SECONDS` | `300` | Real seconds per simulated day (1 sim day = 5 real min) |
| `SIM_START_DATE` | `2026-01-01` | Simulated date at the clock anchor |
| anchor | stored in Postgres table `sim_clock` | Real UTC instant that maps to `SIM_START_DATE 00:00` |

```
speedup  = 86400 / SIM_DAY_SECONDS            # 288 by default
sim_now  = SIM_START + (real_now − anchor) × speedup
```

- The anchor is written **once** by an init step (`INSERT … ON CONFLICT DO NOTHING`) so every
  container, including restarts, agrees on the same simulated time. `make clock-reset` rewrites it.
- **All event timestamps are simulated time.** Spark windows, watermarks, lab `collected_at`,
  report dates and API responses are all in sim time. Real time is only used for
  infrastructure concerns (Prometheus scrape, alert "no data for N real minutes").

Reference conversions at the default speed-up (×288):

| Simulated | Real | Used for |
|---|---|---|
| 1 day | 5 min | Lab file cadence, report cadence |
| 4 hours | 50 s | Sliding trend window length |
| 1 hour | 12.5 s | Tumbling window, sliding step |
| 30 min | ~6 s | Watermark (allowed lateness) |
| ~5 min | ~1 s | Default reading interval per patient |

Window sizes are configured in **simulated** units, so changing `SIM_DAY_SECONDS` keeps the
clinical meaning of the windows intact.

---

## 6. Kafka topic design

Single broker in **KRaft mode** (no ZooKeeper — one less JVM on the laptop). Replication
factor 1 locally; production values noted for the report.

| Topic | Key | Partitions | Retention | Producer | Consumer | Why |
|---|---|---|---|---|---|---|
| `vitals.raw` | `patient_id` | 6 | 7 real days (≈ 2,000 sim days) | vitals simulator | Spark Q1 | Keying by patient guarantees per-patient ordering inside a partition, which trend/slope detection relies on. 6 partitions ≥ Spark local cores, headroom for more patients |
| `labs.raw` | `patient_id` | 3 | unlimited (`retention.ms=-1`) | Airflow `lab_ingest` | Spark Q2 | Tiny volume; kept forever so a full Kappa replay can rebuild lab state |
| `alerts.patient` | `patient_id` | 3 | 7 real days | Spark Q1 | (future) paging/notification service | Alerts are themselves an event stream; decouples notification from processing |
| `deadletter` | source (`vitals`/`labs`) | 1 | 7 real days | simulators' parse layer, Spark, Airflow | ops / replay tool | Invalid records are kept with the reason and original bytes for inspection and replay |

Producer settings: `acks=all`, `enable.idempotence=true`, `linger.ms` small (batching without
hurting latency), JSON values with a `schema_version` field.

---

## 7. PostgreSQL table design (database `ward`)

**Why plain PostgreSQL, not TimescaleDB:** Spark already performs the time-bucketing and
windowed aggregation, so Postgres stores *pre-aggregated* rows (≈ 20 patients × 24 windows per
sim day), not raw high-frequency readings. Timescale's hypertables and continuous aggregates
would duplicate work Spark already does. Raw readings stay in Kafka (the Kappa log) for replay
rather than being copied into Postgres. A second database `airflow` in the same Postgres instance holds Airflow metadata (saves
one container's RAM).

| Table | Primary / unique key | Written by | Purpose |
|---|---|---|---|
| `sim_clock` | singleton | init step | Shared anchor for simulated time |
| `patients` | `patient_id` | seed script | Bed, age band, baseline profile, scenario (dimension table) |
| `vitals_window_1h` | (`patient_id`, `window_start`) | Spark Q1 | Tumbling 1 sim-hour aggregates: count, mean/min/max of each vital, window EWS score |
| `vitals_trend_4h` | (`patient_id`, `window_start`) | Spark Q1 | Sliding 4 h / 1 h-step windows: per-vital linear slope, `trend_flag` (improving / stable / deteriorating) |
| `patient_live_status` | `patient_id` | Spark Q1 | One row per patient: latest vitals, live EWS, lab adjustment, adjusted score, risk tier, `updated_at` — powers the ward API |
| `alerts` | `alert_id` = hash(patient, type, window_start) | Spark Q1 | Threshold and trend alerts; deterministic ID makes re-writes after a restart idempotent |
| `lab_results` | (`patient_id`, `test_type`, `collected_at`) | Spark Q2 | Validated lab rows with parsed `ref_low`/`ref_high`, `abnormal_flag`, `source_file` |
| `lab_latest` (view) | — | — | `DISTINCT ON (patient_id, test_type)` latest result |
| `lab_file_loads` | `sim_date` | Airflow | Load audit + data-quality summary (rows total/valid/rejected, checksum, status). Makes batch loads idempotent: a file with the same checksum is not republished |
| `dead_letter` | (`source`, `kafka_partition`, `kafka_offset`) or (`source_file`, `row_no`) | Spark, Airflow | Queryable copy of rejected records with `error_reason` |
| `daily_risk_report` | (`report_date`, `patient_id`) | Airflow | Final joined features, base score, lab adjustment, trend adjustment, risk tier |
| `pipeline_runs` | (`dag_id`, `run_id`, `task_id`) | Airflow callbacks | Task outcome, duration, sim date — source for batch success/failure and duration metrics |

All writes are **upserts** (`INSERT … ON CONFLICT … DO UPDATE`) keyed on natural keys, so Spark
micro-batch retries and Airflow re-runs never duplicate data.

---

## 8. Processing logic summary (what makes it more than pass-through)

1. **Validation & cleaning** — schema parse; range checks (e.g. HR 20–250, SpO₂ 50–100,
   temp 30–45 °C, systolic > diastolic); missing fields; unknown patient → dead-letter with reason.
2. **Late data** — watermark of 30 sim-min on event time; events older than the watermark are
   dropped by Spark and counted (`numRowsDroppedByWatermark` → Prometheus).
3. **Windowed aggregation** — tumbling 1 sim-hour and sliding 4 sim-hour/1 sim-hour windows.
4. **Trend detection** — least-squares slope per vital inside the sliding window
   (computed with `covar_pop(value, t) / var_pop(t)`), classified against configurable thresholds.
5. **Live early-warning score** — NEWS2-style points for HR, SpO₂, systolic BP, temperature
   (respiratory rate, consciousness and O₂ therapy are not in the feed, so the score is partial
   and documented as such).
6. **Stream/batch join** — each micro-batch is joined with the `lab_latest` snapshot; abnormal
   lactate, CRP, WBC, creatinine, potassium, haemoglobin or troponin add weighted points.
7. **Risk tier** — Low / Medium / High from the adjusted score, with a "single red parameter"
   escalation rule.
8. **Alerts** — threshold breaches (e.g. SpO₂ < 92), high EWS, and sustained deterioration trends.
9. **Daily report** (Airflow) — joins day D's trends and window scores with labs collected on D,
   producing the consolidated per-patient risk table in HTML + CSV.

Scoring bands and lab weights live in **one shared module** (`common/scoring_rules.py`); Spark
builds column expressions from it and the report/API reuse it, which enforces the Kappa
consistency argument in code.

---

## 9. Robustness

| Concern | Mechanism |
|---|---|
| Malformed events | Parse with `from_json` + validity column; invalid → `deadletter` topic + `dead_letter` table |
| Late events | Event-time watermark; dropped-row count exported as a metric |
| Spark crash / restart | Checkpoints per query on a mounted volume; idempotent upserts give exactly-once *effects* |
| Duplicate lab file / DAG re-run | `lab_file_loads` checksum check; `lab_results` natural-key upsert |
| Bad lab rows | Row-level DQ in Airflow; bad rows to dead-letter, file still loads if reject rate < threshold, otherwise quarantined |
| Missing/late lab file | FileSensor timeout + Prometheus rule on "time since last successful lab load" |

---

## 10. Observability plan (detail in Step 8)

- **Ingestion:** events produced / failed per topic (simulator `/metrics`); lab file DQ results
  recorded in `lab_file_loads`.
- **Batch metrics without a Pushgateway:** Airflow tasks are short-lived, so they cannot be
  scraped directly. Instead they record outcomes in Postgres (`lab_file_loads`,
  `pipeline_runs` via success/failure callbacks), and the FastAPI `/metrics` endpoint exposes
  them as gauges (e.g. `lab_last_successful_load_timestamp`, `dag_task_failures_total`). This
  adds no extra container. Trade-off: if the API is down these gauges disappear, which is itself
  caught by an `up == 0` alert on the API target.
- **Processing:** input rate, processing rate, batch duration, rows dropped by watermark,
  invalid-record count, and **consumer lag computed from Spark progress** (Spark tracks its own
  offsets in checkpoints and does not commit to a Kafka consumer group, so a standard
  consumer-group lag exporter would report nothing useful).
- **Storage/serving:** upsert durations, rows written, API request latency, DAG success/failure.
- **Alert rules (minimum):**
  - `VitalsNotReceived` — no vitals consumed for N real minutes (default 2).
  - `HighInvalidRecordRate` — invalid / total > 5 % over 5 real minutes.
  - `LabFileLate` — no successful lab load for > 1.5 × `SIM_DAY_SECONDS`.
- **No Alertmanager:** rules are evaluated by Prometheus and are visible as
  pending/firing on its `/alerts` page and on the Grafana dashboard. Routing to email/pager
  (Alertmanager) is a production improvement, not needed to demonstrate detection.
- Structured JSON logs from every Python component via one shared logger.

---

## 11. Proposed repository layout

```
.
├── docker-compose.yml
├── .env.example
├── Makefile
├── README.md
├── docs/
│   ├── architecture_decision.md      ← this file
│   ├── report_notes.md               (Step 10)
│   └── demo_script.md                (Step 10)
├── common/                           shared package (installed into every image)
│   ├── config.py                     env-driven settings
│   ├── sim_clock.py                  simulated clock
│   ├── logging.py                    structured JSON logging
│   ├── schemas.py                    event/lab schemas + validation rules
│   └── scoring_rules.py              NEWS2-style bands, lab weights, tiers
├── simulators/
│   ├── vitals_producer/              Kafka producer (Step 2)
│   └── lab_generator/                daily lab file drop (Step 3)
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       ├── stream_processor.py       app entry point (Q1 + Q2)
│       └── transforms/               pure functions: validate, window, trend, score, join
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── lab_ingest_dag.py
│       └── daily_risk_report_dag.py
├── api/
│   ├── Dockerfile
│   └── app/                          FastAPI routers, DB access, metrics
├── db/
│   └── init/                         SQL schema, views, seed data
├── observability/
│   ├── prometheus/                   prometheus.yml, alert_rules.yml
│   └── grafana/provisioning/         datasources + dashboard JSON
├── data/                             (git-ignored contents)
│   ├── landing/  archive/  quarantine/  checkpoints/
├── reports/                          generated daily reports (git-ignored)
└── tests/
```

---

## 12. Confirmed decisions

1. Kappa, with Airflow publishing validated lab rows into Kafka.
2. Plain PostgreSQL (TimescaleDB rejected, reasoning in §7).
3. Optional components **not** included: Parquet archive, Pushgateway, Alertmanager.

## 13. Resource budget

Target machine: MacBook Pro (Apple M5, arm64), 16 GB RAM, **8 GB allocated to Docker**.
All images must be multi-arch (arm64-native) to avoid slow emulation. Memory values are
container limits (`mem_limit`); typical usage is lower.

| Service | Limit | Tuning |
|---|---|---|
| Kafka (KRaft, 1 broker) | 768 MB | JVM heap 512 MB |
| PostgreSQL (`ward` + `airflow` DBs) | 512 MB | small `shared_buffers` (128 MB) |
| Spark (1 container, `local[4]`, 2 queries) | 2 GB | driver memory 1 GB, `spark.sql.shuffle.partitions=6` |
| Airflow scheduler (LocalExecutor) | 768 MB | `parallelism` 4 |
| Airflow webserver | 768 MB | 1–2 workers |
| FastAPI | 256 MB | |
| Vitals simulator | 256 MB | |
| Lab simulator | 128 MB | |
| Prometheus | 256 MB | 2-day retention |
| Grafana | 256 MB | |
| **Total limits** | **≈ 6.0 GB** | leaves ~2 GB for the Docker VM and spikes |

Why these choices save memory: Spark runs in **local mode** in one JVM instead of a
master + worker cluster; Kafka uses **KRaft** (no ZooKeeper JVM); Airflow uses
**LocalExecutor** (no Redis/Celery workers) and shares the Postgres instance.
