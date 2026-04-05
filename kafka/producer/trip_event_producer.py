"""
Kafka Producer: NYC Taxi Trip Event Simulator
=============================================
Simulates a real-time stream of NYC Yellow Taxi trip events by generating
synthetic trip records that match the TLC schema. In a real MNC deployment,
this would be replaced by a live feed from the taxi dispatch system or
cab app backend (e.g., a Debezium CDC connector on the operational DB).

Usage:
    python trip_event_producer.py [--tps 100] [--duration 60]

    --tps       : Events to produce per second (default: 100)
    --duration  : How many seconds to run (default: infinite)
    --topic     : Kafka topic name (default: nyc-taxi-trips-stream)
    --brokers   : Kafka bootstrap servers (default: localhost:9092)

Environment variables (override CLI args):
    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TOPIC

Install:
    pip install confluent-kafka faker
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NYC Taxi domain constants (matches TLC data schema)
# ---------------------------------------------------------------------------
VENDOR_IDS = [1, 2]
RATE_CODE_IDS = [1, 2, 3, 4, 5, 6]
PAYMENT_TYPES = [1, 2, 3, 4]       # 1=Credit card, 2=Cash, 3=No charge, 4=Dispute
LOCATION_IDS = list(range(1, 266))  # 265 NYC taxi zones
AIRPORT_LOCATION_IDS = [1, 132, 138]  # EWR, JFK, LaGuardia

# Realistic fare ranges by trip type
FARE_CONFIG = {
    "short":   {"distance": (0.5, 2.5),  "fare": (5.0, 15.0),   "duration_min": (3, 12)},
    "medium":  {"distance": (2.5, 8.0),  "fare": (15.0, 35.0),  "duration_min": (10, 30)},
    "long":    {"distance": (8.0, 25.0), "fare": (35.0, 80.0),  "duration_min": (25, 60)},
    "airport": {"distance": (10.0, 20.0),"fare": (52.0, 80.0),  "duration_min": (30, 75)},
}


def _generate_trip_event(now: datetime) -> dict[str, Any]:
    """Generate a single synthetic NYC taxi trip event."""
    trip_type = random.choices(
        ["short", "medium", "long", "airport"],
        weights=[0.45, 0.35, 0.15, 0.05],
    )[0]

    cfg = FARE_CONFIG[trip_type]

    # Trip timing
    duration_minutes = random.uniform(*cfg["duration_min"])
    pickup_dt = now - timedelta(minutes=random.uniform(0, 2))  # slight jitter
    dropoff_dt = pickup_dt + timedelta(minutes=duration_minutes)

    # Locations
    if trip_type == "airport":
        pickup_loc = random.choice(AIRPORT_LOCATION_IDS)
        dropoff_loc = random.choice([l for l in LOCATION_IDS if l not in AIRPORT_LOCATION_IDS])
        rate_code = 2
    else:
        pickup_loc = random.choice(LOCATION_IDS)
        dropoff_loc = random.choice(LOCATION_IDS)
        rate_code = random.choices(RATE_CODE_IDS, weights=[0.85, 0.05, 0.03, 0.03, 0.02, 0.02])[0]

    # Fares
    fare = round(random.uniform(*cfg["fare"]), 2)
    distance = round(random.uniform(*cfg["distance"]), 2)
    payment_type = random.choices(PAYMENT_TYPES, weights=[0.65, 0.30, 0.03, 0.02])[0]

    # Tips only for credit card payments
    tip = round(fare * random.uniform(0.10, 0.25), 2) if payment_type == 1 else 0.0
    mta_tax = 0.5
    improvement_surcharge = 0.3
    congestion_surcharge = 2.5 if pickup_loc in range(1, 70) else 0.0  # Manhattan
    airport_fee = 1.25 if trip_type == "airport" else 0.0
    extra = random.choice([0.0, 0.5, 1.0])
    total = round(fare + tip + mta_tax + improvement_surcharge + congestion_surcharge + airport_fee + extra, 2)

    return {
        "event_id": str(uuid.uuid4()),
        "vendorid": random.choice(VENDOR_IDS),
        "tpep_pickup_datetime": pickup_dt.isoformat(),
        "tpep_dropoff_datetime": dropoff_dt.isoformat(),
        "passenger_count": random.choices([1, 2, 3, 4, 5, 6], weights=[0.60, 0.20, 0.08, 0.06, 0.04, 0.02])[0],
        "trip_distance": distance,
        "ratecodeid": rate_code,
        "store_and_fwd_flag": random.choices(["Y", "N"], weights=[0.02, 0.98])[0],
        "pulocationid": pickup_loc,
        "dolocationid": dropoff_loc,
        "payment_type": payment_type,
        "fare_amount": fare,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip,
        "tolls_amount": round(random.choices([0.0, 6.55, 13.10], weights=[0.80, 0.15, 0.05])[0], 2),
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total,
        "congestion_surcharge": congestion_surcharge,
        "airport_fee": airport_fee,
    }


def _delivery_callback(err, msg) -> None:
    """Called once per message after it is acknowledged by the broker."""
    if err:
        log.error("Message delivery failed: %s", err)
    else:
        log.debug("Delivered to %s [%d] @ offset %d", msg.topic(), msg.partition(), msg.offset())


def _ensure_topic_exists(brokers: str, topic: str, num_partitions: int = 3) -> None:
    """Create the Kafka topic if it doesn't exist."""
    admin = AdminClient({"bootstrap.servers": brokers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        log.info("Creating Kafka topic: %s (partitions=%d)", topic, num_partitions)
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        futures = admin.create_topics([new_topic])
        for t, f in futures.items():
            try:
                f.result()
                log.info("Topic '%s' created successfully", t)
            except Exception as exc:
                log.warning("Could not create topic '%s': %s", t, exc)
    else:
        log.info("Topic '%s' already exists", topic)


class TripEventProducer:
    """Produces synthetic NYC taxi trip events to a Kafka topic at a target TPS."""

    def __init__(self, brokers: str, topic: str, tps: int):
        self.topic = topic
        self.tps = tps
        self._running = False
        self._total_produced = 0
        self._total_errors = 0

        conf = {
            "bootstrap.servers": brokers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 300,
            "compression.type": "lz4",
            "batch.size": 65536,
            "linger.ms": 50,
            "queue.buffering.max.messages": 100_000,
        }
        self._producer = Producer(conf)
        _ensure_topic_exists(brokers, topic)

    def produce(self, duration_seconds: float | None = None) -> None:
        """
        Produce events to Kafka at self.tps rate.
        Runs indefinitely if duration_seconds is None.
        """
        self._running = True
        sleep_interval = 1.0 / self.tps
        start_time = time.monotonic()
        last_stats_time = start_time

        log.info("Starting producer: topic=%s tps=%d", self.topic, self.tps)

        while self._running:
            now = datetime.utcnow()
            event = _generate_trip_event(now)

            try:
                self._producer.produce(
                    topic=self.topic,
                    key=event["event_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=_delivery_callback,
                )
                self._total_produced += 1
            except BufferError:
                # Broker backpressure — poll to flush
                self._producer.poll(0.1)
            except Exception as exc:
                log.error("Produce error: %s", exc)
                self._total_errors += 1

            # Non-blocking poll to serve delivery callbacks
            self._producer.poll(0)

            # Print throughput stats every 10 seconds
            elapsed = time.monotonic() - last_stats_time
            if elapsed >= 10:
                actual_tps = self._total_produced / (time.monotonic() - start_time)
                log.info(
                    "Stats: produced=%d errors=%d actual_tps=%.1f",
                    self._total_produced, self._total_errors, actual_tps,
                )
                last_stats_time = time.monotonic()

            # Duration check
            if duration_seconds and (time.monotonic() - start_time) >= duration_seconds:
                log.info("Duration limit reached (%.0fs). Stopping.", duration_seconds)
                break

            time.sleep(sleep_interval)

        self._flush()

    def stop(self) -> None:
        self._running = False

    def _flush(self) -> None:
        log.info("Flushing remaining messages to broker...")
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            log.warning("%d messages could not be delivered", remaining)
        log.info("Producer finished. Total produced: %d | Errors: %d", self._total_produced, self._total_errors)


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC Taxi Trip Kafka Producer")
    parser.add_argument("--tps", type=int, default=100, help="Events per second (default: 100)")
    parser.add_argument("--duration", type=float, default=None, help="Run duration in seconds (default: infinite)")
    parser.add_argument("--topic", type=str, default=os.getenv("KAFKA_TOPIC", "nyc-taxi-trips-stream"))
    parser.add_argument("--brokers", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    args = parser.parse_args()

    producer = TripEventProducer(brokers=args.brokers, topic=args.topic, tps=args.tps)

    def _handle_signal(sig, frame):
        log.info("Shutdown signal received.")
        producer.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    producer.produce(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
