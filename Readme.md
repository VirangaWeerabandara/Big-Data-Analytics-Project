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

_Steps 3–7 (lab files, streaming job, DAGs, API) will be documented here as they are built._

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
