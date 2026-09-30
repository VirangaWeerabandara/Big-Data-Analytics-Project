"""Connectivity smoke check for the Spark container.

Proves the three things the streaming job (Step 4) depends on:
  1. the shared simulated clock is readable from Postgres,
  2. a local-mode SparkSession starts,
  3. the baked-in Kafka connector can read the vitals topic.

Exits 0 on success. Run with `make spark-smoke`.
"""

from __future__ import annotations

import logging
import sys

import psycopg
from pyspark.sql import SparkSession

from ward_common.config import ClockSettings, KafkaSettings, PostgresSettings
from ward_common.log import configure_logging
from ward_common.sim_clock import ClockReader, describe

configure_logging("spark-smoke-check")
logging.getLogger("py4j").setLevel(logging.WARNING)
log = logging.getLogger("spark.smoke_check")


def main() -> int:
    kafka = KafkaSettings.from_env()
    pg = PostgresSettings.from_env()

    reader = ClockReader(
        lambda: psycopg.connect(**pg.connect_kwargs(), connect_timeout=5),
        ClockSettings.from_env().refresh_seconds,
    )
    log.info("sim_clock_ok", extra=describe(reader.wait_until_ready()))

    spark = SparkSession.builder.appName("ward-smoke-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        records = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
            .option("subscribe", kafka.topic_vitals)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
            .count()
        )
        log.info(
            "kafka_read_ok",
            extra={
                "topic": kafka.topic_vitals,
                "records": records,
                "spark_version": spark.version,
                "master": spark.sparkContext.master,
            },
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
