# Ward Vital Signs Monitoring — Big Data Pipeline

Near-real-time monitoring of bedside patient vitals, correlated daily with pathology lab
results, to answer: **which patients show concerning vital-sign trends right now, and how do
yesterday's lab results change their risk going forward?**

> **Disclaimer.** All data is synthetic. The risk score is a simplified, NEWS2-*style*
> illustration built for a data-engineering exercise. It is **not** a clinical tool and must not
> be used for any medical decision.

## Architecture

Kappa architecture: every input (vitals and lab rows) flows through Kafka, and one Spark
Structured Streaming application produces every derived table. Airflow ingests the daily lab
file into Kafka and schedules the daily report; FastAPI serves the results; Prometheus and
Grafana observe every stage.

Full decision record, diagram, topic and table design:
[docs/architecture_decision.md](docs/architecture_decision.md).

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka 4.1 (KRaft, single broker) |
| Stream processing | Apache Spark 4.1 Structured Streaming (local mode) |
| Orchestration | Apache Airflow 3.1 (LocalExecutor) |
| Storage | PostgreSQL 17 |
| Serving | FastAPI |
| Observability | Structured JSON logs, Prometheus, Grafana |

## Prerequisites

- Docker Desktop with **at least 8 GB** of memory assigned (Settings → Resources)
- GNU Make and Python 3.10+ (for `make env` and the unit tests)

## Quick start

```bash
make env      # create .env with generated secrets (once)
make up       # build images and start the stack
make ps       # all long-running services should be "healthy"
```

| Service | URL | Login |
|---|---|---|
| Ward API | http://localhost:8000/docs | – |
| Airflow | http://localhost:8080 | `admin` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Grafana | http://localhost:3000 | `admin` / `GRAFANA_ADMIN_PASSWORD` from `.env` |
| Prometheus | http://localhost:9090 | – |
| Spark UI | http://localhost:4040 | while the streaming job runs |
| Postgres | `localhost:5433`, db `ward` | `ward` / `WARD_DB_PASSWORD` |
| Kafka (from host) | `localhost:9094` | – |

Stop with `make down` (keeps data, pauses the simulated clock). `make clean` deletes all data.
Run `make help` for every target.

## Simulated clock

Default: **1 simulated day = 5 real minutes** (`SIM_DAY_SECONDS=300`, a ×288 speed-up).

```
sim_now = anchor_sim + (real_now − anchor_real) × 86400 / SIM_DAY_SECONDS
```

The anchor is stored in the Postgres table `sim_clock`, so every container agrees on the
simulated time. All event timestamps, windows, lab dates and reports use simulated time.
`make down` pauses the clock and `make up` resumes it without a jump.

| Simulated | Real (default) |
|---|---|
| 1 day | 5 min |
| 1 hour | 12.5 s |
| ~5 min | ~1 s |

Inspect it with `make clock-show`, or in SQL with `SELECT sim_now();`.

## Running the pipeline

### Vitals simulator (streaming source)

Starts automatically with `make up` as the `vitals-simulator` service and publishes JSON
readings to `vitals.raw`, keyed by `patient_id`.

```json
{"schema_version":1,"event_id":"46fe7133-…","patient_id":"P009","heart_rate":65,"spo2":98,
 "systolic_bp":97,"diastolic_bp":65,"temperature":36.5,
 "timestamp":"2026-01-04T07:01:41.695Z","produced_at":"2026-09-30T12:03:54.120Z"}
```

`timestamp` is simulated event time (used for windows); `produced_at` is real time (used only
for latency). Each patient follows a hidden storyline — `stable`, `sepsis`,
`respiratory_failure`, `haemorrhage` or `recovering` — with gradual, recurring deterioration
episodes, plus random short spikes. The storyline is stored in the `patients` table as ground
truth for evaluation and is never sent in the stream.

Configuration (`.env` or CLI flags):

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `SIM_PATIENTS` | `--patients` | 20 | Patients on the ward |
| `SIM_RATE` | `--rate` | 20 | Total readings per real second |
| `SIM_SEED` | `--seed` | 42 | Same seed → same patients and storylines |
| `SIM_SPIKE_RATE` | `--spike-rate` | 0.01 | Chance a transient spike starts per reading |
| `SIM_MALFORMED_RATE` | `--malformed-rate` | 0.02 | Broken payloads (→ dead-letter) |
| `SIM_LATE_RATE` | `--late-rate` | 0.02 | Readings delivered 5–120 sim-minutes late |
| `SIM_DUPLICATE_RATE` | `--duplicate-rate` | 0.01 | Readings sent twice with the same `event_id` |

Useful commands:

```bash
make sim-dry                  # print 5 s of readings, no Kafka/DB needed
make consume n=5              # newest messages on vitals.raw
make offsets                  # messages per partition
make logs s=vitals-simulator  # includes deterioration_started / _resolved events
curl localhost:8001/metrics   # producer metrics
```

### Lab simulator (daily batch source)

The `lab-simulator` service writes one CSV per simulated day to `data/landing/`:
`labs_YYYY-MM-DD.csv` holds results **collected on** that day and is uploaded at 06:00
(simulated) the next morning — "yesterday's labs".

```csv
sample_id,patient_id,test_type,result_value,unit,reference_range,collected_at
20260104-P014-AM,P014,CRP,181.9,mg/L,<5,2026-01-04T07:41:12Z
```

The brief's five columns plus `sample_id` (one blood sample → several tests) and `unit`.

| Test | Unit | Reference range |
|---|---|---|
| WBC | 10^9/L | 4.0–11.0 |
| CRP | mg/L | <5 |
| LACTATE | mmol/L | 0.5–2.0 |
| CREATININE | umol/L | 59–104 (M), 45–84 (F) |
| POTASSIUM | mmol/L | 3.5–5.3 |
| HAEMOGLOBIN | g/L | 130–180 (M), 115–165 (F) |
| TROPONIN | ng/L | <14 |

- Results follow the same hidden storyline as the vitals (same seed), with realistic lags:
  lactate and haemoglobin respond immediately, CRP peaks ~18 simulated hours later.
- Everyone gets morning bloods (routine panel); unwell patients also get lactate/troponin and
  an evening re-check.
- ~3% bad rows: missing or unknown patient, non-numeric (`haemolysed`), implausible values,
  unknown test codes, malformed reference ranges or timestamps, wrong day, duplicate rows.
- ~10% of files arrive 2–8 simulated hours late and ~5% never arrive.
- Files are written atomically (temp file + rename) and are byte-for-byte reproducible.

| Variable | Default | Meaning |
|---|---|---|
| `LAB_UPLOAD_HOUR` | 6 | Simulated hour on D+1 when day D's file is uploaded |
| `LAB_LATE_RATE` / `LAB_LATE_MAX_HOURS` | 0.10 / 8 | Late uploads |
| `LAB_MISSING_RATE` | 0.05 | Files that never arrive |
| `LAB_BAD_ROW_RATE` | 0.03 | Corrupted rows |

```bash
make landing                       # files in landing/ archive/ quarantine/
make lab-dry d=2026-01-04          # print a day's CSV without writing it
make lab-day d=2026-01-04 f=--force   # (re)deliver a day now, e.g. after a "missing" day
curl localhost:8002/metrics        # lab_files_written_total, lab_files_withheld_total, ...
```

### Stream processing (Spark)

One Spark application (`spark` service, local mode) runs five streaming queries:

| Query | Input | Output |
|---|---|---|
| `vitals_quality` | `vitals.raw` | validity counts (metrics); invalid records → `deadletter` |
| `vitals_windows` | `vitals.raw` | 1 h windows + EWS + lab join → `vitals_window_1h`, `patient_live_status`, `alerts` (+ `alerts.patient`) |
| `vitals_trends` | `vitals.raw` | 4 h/1 h sliding slopes → `vitals_trend_4h`, trend alerts |
| `labs` | `labs.raw` | `lab_results` (invalid → `deadletter`) |
| `deadletter_sink` | `deadletter` | `dead_letter` table |

Windows and the watermark are in simulated time (1 h window = 12.5 real s; watermark
30 sim-min = 6.25 real s). Scoring rules live in `common/ward_common/scoring_rules.py`
(illustrative NEWS2-style score — not clinical).

```bash
make stream-status        # row counts of the derived tables
make logs s=spark         # alert_raised events, query progress
curl localhost:8003/metrics | grep ^spark_   # lag, batch duration, watermark drops, ...
make stream-reset         # Kappa replay: wipe derived tables + checkpoints, rebuild from Kafka
make spark-test           # Spark unit/parity tests (run inside the Spark image)
```

Changing window sizes, the watermark or `spark.sql.shuffle.partitions` changes the state
layout, so it needs `make stream-reset`.

### Lab ingestion (Airflow)

DAG `lab_ingest` (Airflow UI → http://localhost:8080) runs once per simulated day:

1. `resolve_target_day` — yesterday in simulated time.
2. `wait_for_lab_file` — sensor (reschedule mode). If the file is not in `landing/` by
   06:00 + `LAB_SLA_HOURS` (simulated), the day is recorded as **missing** and the task fails.
3. `load_landing_files` — runs regardless and processes every waiting file: sha256 checksum,
   header check, row validation (shared validator + in-file duplicates), then either publishes
   valid rows to `labs.raw` and rejected rows to `deadletter`, or quarantines the whole file
   (> `LAB_MAX_REJECT_RATE` bad rows). Files end up in `data/archive/` or `data/quarantine/`.

Nothing in Airflow writes lab results to Postgres: Spark consumes `labs.raw` (Kappa).

```bash
make lab-loads                                   # per-day ledger: status, arrival, DQ counts
make dag-trigger d=lab_ingest                    # run now instead of waiting
make lab-day d=2026-01-10 f=--force              # identical re-delivery -> skipped as duplicate
make lab-day d=2026-01-02 f="--bad-row-rate 0.6" # mostly-bad file -> quarantined
docker compose stop lab-simulator                # -> next day recorded as missing
```

| Variable | Default | Meaning |
|---|---|---|
| `LAB_SLA_HOURS` | 4 | Simulated hours after the 06:00 upload before a file counts as missing |
| `LAB_MAX_REJECT_RATE` | 0.2 | Quarantine the file above this fraction of bad rows |
| `LAB_POKE_SECONDS` | 10 | Sensor poke interval (real seconds) |

_Steps 6–7 (risk report, API) will be documented here as they are built._

## Reproducing results

_To be completed in Step 10._

## Tests

```bash
make venv     # once
make test
```

## Repository layout

See [docs/architecture_decision.md §11](docs/architecture_decision.md#11-repository-layout).

## Assumptions and limitations

_To be completed in Step 10._

## Individual contributions

_Placeholder — to be completed by the team._
