"""Ward monitoring API.

Scaffold: /health and /metrics only. Ward endpoints are added in Step 7.
"""

from __future__ import annotations

import logging

import psycopg
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ward_common.config import ClockSettings, PostgresSettings
from ward_common.log import configure_logging
from ward_common.sim_clock import ClockReader

configure_logging("api")
log = logging.getLogger(__name__)

pg_settings = PostgresSettings.from_env()


def connect() -> psycopg.Connection:
    return psycopg.connect(**pg_settings.connect_kwargs(), connect_timeout=3)


clock_reader = ClockReader(connect, ClockSettings.from_env().refresh_seconds)

app = FastAPI(title="Ward Vitals Monitoring API", version="0.1.0")


@app.get("/health")
def health(response: Response) -> dict:
    """Liveness plus dependency checks; 503 if any dependency is down."""
    checks: dict[str, str] = {}
    sim_now = None

    try:
        with connect() as conn:
            conn.execute("SELECT 1")
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {type(exc).__name__}"
        log.warning("health_database_failed", exc_info=True)

    try:
        sim_now = clock_reader.now().isoformat()
        checks["sim_clock"] = "ok"
    except Exception as exc:
        checks["sim_clock"] = f"error: {type(exc).__name__}"

    healthy = all(value == "ok" for value in checks.values())
    if not healthy:
        response.status_code = 503
    return {"status": "ok" if healthy else "degraded", "checks": checks, "sim_now": sim_now}


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
