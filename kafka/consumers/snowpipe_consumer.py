"""
Kafka → Snowpipe Consumer
=========================
Reads NYC taxi trip events from a Kafka topic in micro-batches, writes them
as JSON files to a Snowflake internal stage, then triggers Snowpipe via the
REST Ingest API to load into raw_data.nyc_taxi_trips_streaming.

Design decisions:
  - Micro-batch pattern: accumulate N events or T seconds, then flush.
    This balances latency (small batches = lower lag) vs cost (fewer files =
    less Snowpipe overhead). Default: flush every 500 events OR 30 seconds.
  - At-least-once delivery: Kafka offsets are committed AFTER successful
    Snowpipe trigger. A duplicate event_id is acceptable; the silver model
    deduplicates via the trip_id surrogate key.
  - Backpressure handling: if Snowpipe returns 429, exponential backoff.

Usage:
    python snowpipe_consumer.py [--batch-size 500] [--flush-interval 30]

Environment variables (required):
    KAFKA_BOOTSTRAP_SERVERS   - e.g., localhost:9092
    KAFKA_TOPIC               - e.g., nyc-taxi-trips-stream
    KAFKA_GROUP_ID            - consumer group id
    SNOWFLAKE_ACCOUNT         - e.g., abc12345.us-east-1
    SNOWFLAKE_USER            - service account user
    SNOWFLAKE_PASSWORD        - service account password
    SNOWFLAKE_DATABASE        - nyc_taxi_lakehouse
    SNOWFLAKE_SCHEMA          - raw_data
    SNOWFLAKE_STAGE           - taxi_streaming_stage
    SNOWFLAKE_PIPE            - nyc_taxi_lakehouse.raw_data.taxi_streaming_pipe

Install:
    pip install confluent-kafka snowflake-ingest snowflake-connector-python
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import signal
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from snowflake.ingest import SimpleIngestManager, StagedFile
from snowflake.connector import connect as snowflake_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("snowpipe_consumer")


# ---------------------------------------------------------------------------
# Configuration (from environment)
# ---------------------------------------------------------------------------
def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Required environment variable not set: {key}")
    return val


class ConsumerConfig:
    kafka_brokers: str      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str        = os.getenv("KAFKA_TOPIC", "nyc-taxi-trips-stream")
    kafka_group_id: str     = os.getenv("KAFKA_GROUP_ID", "snowpipe-consumer-group")
    sf_account: str         = os.getenv("SNOWFLAKE_ACCOUNT", "")
    sf_user: str            = os.getenv("SNOWFLAKE_USER", "")
    sf_password: str        = os.getenv("SNOWFLAKE_PASSWORD", "")
    sf_database: str        = os.getenv("SNOWFLAKE_DATABASE", "nyc_taxi_lakehouse")
    sf_schema: str          = os.getenv("SNOWFLAKE_SCHEMA", "raw_data")
    sf_stage: str           = os.getenv("SNOWFLAKE_STAGE", "taxi_streaming_stage")
    sf_pipe: str            = os.getenv("SNOWFLAKE_PIPE", "nyc_taxi_lakehouse.raw_data.taxi_streaming_pipe")
    sf_warehouse: str       = os.getenv("SNOWFLAKE_WAREHOUSE", "nyc_taxi_wh")
    batch_size: int         = int(os.getenv("BATCH_SIZE", "500"))
    flush_interval_sec: int = int(os.getenv("FLUSH_INTERVAL_SEC", "30"))
    max_retries: int        = 3


# ---------------------------------------------------------------------------
# Snowflake Snowpipe ingest manager
# ---------------------------------------------------------------------------
class SnowpipeLoader:
    """Handles staging files to Snowflake and triggering Snowpipe ingestion."""

    def __init__(self, cfg: ConsumerConfig):
        self._cfg = cfg
        # Snowflake connector for PUT (stage upload)
        self._sf_conn = snowflake_connect(
            account=cfg.sf_account,
            user=cfg.sf_user,
            password=cfg.sf_password,
            database=cfg.sf_database,
            schema=cfg.sf_schema,
            warehouse=cfg.sf_warehouse,
        )
        # Snowpipe REST ingest manager
        self._ingest_mgr = SimpleIngestManager(
            account=cfg.sf_account,
            host=f"{cfg.sf_account}.snowflakecomputing.com",
            user=cfg.sf_user,
            pipe=cfg.sf_pipe,
            private_key=None,           # Use password auth (simpler for local dev)
            scheme="https",
            port=443,
        )
        log.info("SnowpipeLoader initialized: pipe=%s stage=%s", cfg.sf_pipe, cfg.sf_stage)

    def upload_and_ingest(self, events: list[dict[str, Any]]) -> str:
        """
        Serialize events to a gzipped JSON file, PUT to Snowflake stage,
        then trigger Snowpipe to load. Returns the staged filename.
        """
        batch_id = uuid.uuid4().hex[:12]
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        filename = f"trips_stream_{ts}_{batch_id}.json.gz"

        # Write events as a JSON array to a temp gzip file
        with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
            tmp_path = tmp.name
            with gzip.open(tmp_path, "wt", encoding="utf-8") as gz_file:
                json.dump(events, gz_file)

        try:
            # PUT the file to Snowflake internal stage
            cursor = self._sf_conn.cursor()
            stage_path = f"@{self._cfg.sf_database}.{self._cfg.sf_schema}.{self._cfg.sf_stage}"
            put_sql = f"PUT 'file://{tmp_path}' {stage_path}/{filename} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            log.debug("PUT: %s", put_sql)
            cursor.execute(put_sql)
            rows = cursor.fetchall()
            cursor.close()

            for row in rows:
                status = row[6] if len(row) > 6 else "UNKNOWN"
                if status not in ("UPLOADED", "SKIPPED"):
                    raise RuntimeError(f"Stage upload failed for {filename}: status={status}")

            log.info("Staged %d events → %s/%s", len(events), stage_path, filename)

            # Trigger Snowpipe to ingest the staged file
            for attempt in range(self._cfg.max_retries):
                try:
                    staged_file = StagedFile(filename, None)
                    response = self._ingest_mgr.ingest_files([staged_file])
                    log.info("Snowpipe triggered: %s | response=%s", filename, response)
                    return filename
                except Exception as exc:
                    wait = 2 ** attempt
                    log.warning(
                        "Snowpipe trigger failed (attempt %d/%d): %s — retrying in %ds",
                        attempt + 1, self._cfg.max_retries, exc, wait,
                    )
                    time.sleep(wait)

            raise RuntimeError(f"Snowpipe trigger failed after {self._cfg.max_retries} attempts")

        finally:
            import os as _os
            _os.unlink(tmp_path)

    def close(self) -> None:
        self._sf_conn.close()


# ---------------------------------------------------------------------------
# Kafka Consumer
# ---------------------------------------------------------------------------
class TripStreamConsumer:
    """
    Consumes NYC taxi trip events from Kafka and loads them to Snowflake
    via Snowpipe in micro-batches.
    """

    def __init__(self, cfg: ConsumerConfig):
        self._cfg = cfg
        self._loader = SnowpipeLoader(cfg)
        self._running = False
        self._total_consumed = 0
        self._total_batches = 0

        kafka_conf = {
            "bootstrap.servers": cfg.kafka_brokers,
            "group.id": cfg.kafka_group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,         # Manual commit after successful load
            "max.poll.interval.ms": 300_000,     # 5 min (account for slow Snowpipe)
            "session.timeout.ms": 30_000,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,
        }
        self._consumer = Consumer(kafka_conf)
        self._consumer.subscribe([cfg.kafka_topic])
        log.info(
            "Consumer initialized: topic=%s group=%s brokers=%s",
            cfg.kafka_topic, cfg.kafka_group_id, cfg.kafka_brokers,
        )

    def run(self) -> None:
        """Main consume loop. Runs until stop() is called."""
        self._running = True
        batch: list[dict[str, Any]] = []
        batch_start = time.monotonic()
        last_offsets: dict[int, int] = {}  # partition → offset

        log.info("Consumer started. batch_size=%d flush_interval=%ds",
                 self._cfg.batch_size, self._cfg.flush_interval_sec)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # No message — check if flush interval exceeded
                    if batch and (time.monotonic() - batch_start) >= self._cfg.flush_interval_sec:
                        self._flush_batch(batch, last_offsets)
                        batch = []
                        last_offsets = {}
                        batch_start = time.monotonic()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                # Deserialize and enrich with Kafka metadata
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    event["kafka_metadata"] = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": datetime.fromtimestamp(
                            msg.timestamp()[1] / 1000, tz=timezone.utc
                        ).isoformat(),
                    }
                    batch.append(event)
                    last_offsets[msg.partition()] = msg.offset()
                    self._total_consumed += 1
                except (json.JSONDecodeError, Exception) as exc:
                    log.warning("Failed to deserialize message at offset %d: %s", msg.offset(), exc)
                    continue

                # Flush when batch is full or time interval exceeded
                should_flush = (
                    len(batch) >= self._cfg.batch_size
                    or (time.monotonic() - batch_start) >= self._cfg.flush_interval_sec
                )
                if should_flush:
                    self._flush_batch(batch, last_offsets)
                    batch = []
                    last_offsets = {}
                    batch_start = time.monotonic()

        except KeyboardInterrupt:
            log.info("Interrupt received, shutting down...")
        finally:
            # Flush any remaining events
            if batch:
                self._flush_batch(batch, last_offsets)
            self._consumer.close()
            self._loader.close()
            log.info(
                "Consumer shutdown. total_consumed=%d total_batches=%d",
                self._total_consumed, self._total_batches,
            )

    def _flush_batch(self, batch: list[dict], last_offsets: dict[int, int]) -> None:
        """Upload batch to Snowflake stage and trigger Snowpipe. Commit offsets on success."""
        if not batch:
            return
        log.info("Flushing batch of %d events to Snowpipe...", len(batch))
        try:
            filename = self._loader.upload_and_ingest(batch)
            # Commit offsets only after successful Snowpipe trigger (at-least-once)
            tps = [
                TopicPartition(self._cfg.kafka_topic, part, off + 1)
                for part, off in last_offsets.items()
            ]
            self._consumer.commit(offsets=tps, asynchronous=False)
            self._total_batches += 1
            log.info(
                "Batch flushed: file=%s events=%d batch_total=%d",
                filename, len(batch), self._total_batches,
            )
        except Exception as exc:
            log.error("Batch flush FAILED: %s — offsets NOT committed, will reprocess", exc)

    def stop(self) -> None:
        self._running = False


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Kafka → Snowpipe Consumer")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    parser.add_argument("--flush-interval", type=int, default=int(os.getenv("FLUSH_INTERVAL_SEC", "30")))
    args = parser.parse_args()

    cfg = ConsumerConfig()
    cfg.batch_size = args.batch_size
    cfg.flush_interval_sec = args.flush_interval

    consumer = TripStreamConsumer(cfg)

    def _handle_signal(sig, frame):
        log.info("Shutdown signal received.")
        consumer.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer.run()


if __name__ == "__main__":
    main()
