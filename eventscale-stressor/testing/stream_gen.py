"""
High-Performance IoT Data Stream Generator
==========================================
Simulates a smart city IoT network with:
  - Temperature / humidity / pressure sensors
  - Smart energy meters
  - Vehicle GPS trackers
  - Industrial machine vibration monitors
  - Air quality stations

Target: 20,000+ events/second
Strategy: multiprocessing + batch generation + orjson serialisation
"""

import os
import sys
import time
import json
import random
import signal
import argparse
import itertools
import threading
import multiprocessing as mp
from datetime import datetime, timezone
from collections import deque
from typing import Generator
from kafka import KafkaProducer
import json
from typing import Any

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# ═══════════════════════════════════════════════════════════════════════════
#  DEVICE REGISTRIES  (pre-built once per worker to avoid repeated alloc)
# ═══════════════════════════════════════════════════════════════════════════

SENSOR_TYPES = ["temperature", "humidity", "pressure", "air_quality", "energy_meter",
                "gps_tracker", "vibration", "light_level", "water_flow", "noise_level"]

LOCATIONS = [
    "factory_floor_a", "factory_floor_b", "warehouse_1", "warehouse_2",
    "office_north", "office_south", "parking_lot", "rooftop",
    "server_room", "lobby", "loading_dock", "data_center",
    "street_sensor_1", "street_sensor_2", "bridge_monitor",
]

DEVICE_STATUS = ["online", "online", "online", "online", "degraded", "warning"]  # weighted

ALERT_LEVELS = [None, None, None, None, None, "info", "warning", "critical"]     # mostly None

VEHICLE_IDS  = [f"VH-{i:04d}" for i in range(1, 201)]
MACHINE_IDS  = [f"MCH-{i:03d}" for i in range(1, 51)]
METER_IDS    = [f"MTR-{i:04d}" for i in range(1, 301)]
SENSOR_IDS   = [f"SNS-{i:05d}" for i in range(1, 1001)]

_RNG = random.Random()   # each process gets its own after fork


# ═══════════════════════════════════════════════════════════════════════════
#  EVENT GENERATORS  (fast, no per-call allocations where possible)
# ═══════════════════════════════════════════════════════════════════════════

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def gen_env_sensor(rng: random.Random) -> dict:
    loc = rng.choice(LOCATIONS)
    stype = rng.choice(["temperature", "humidity", "pressure", "light_level", "noise_level"])
    value_map = {
        "temperature":  round(rng.uniform(-10, 55), 2),
        "humidity":     round(rng.uniform(20, 95), 2),
        "pressure":     round(rng.uniform(950, 1060), 2),
        "light_level":  round(rng.uniform(0, 2000), 1),
        "noise_level":  round(rng.uniform(30, 110), 1),
    }
    unit_map = {
        "temperature": "°C", "humidity": "%RH", "pressure": "hPa",
        "light_level": "lux", "noise_level": "dB"
    }
    v = value_map[stype]
    return {
        "event_type": "env_sensor",
        "sensor_id":  rng.choice(SENSOR_IDS),
        "sensor_type": stype,
        "location":   loc,
        "value":      v,
        "unit":       unit_map[stype],
        "status":     rng.choice(DEVICE_STATUS),
        "alert":      rng.choice(ALERT_LEVELS),
        "battery_pct": rng.randint(5, 100),
        "firmware":   f"v{rng.randint(1,3)}.{rng.randint(0,9)}.{rng.randint(0,20)}",
        "ts":         _ts(),
    }


def gen_energy_meter(rng: random.Random) -> dict:
    return {
        "event_type":    "energy_meter",
        "meter_id":      rng.choice(METER_IDS),
        "location":      rng.choice(LOCATIONS),
        "voltage_v":     round(rng.uniform(218, 242), 2),
        "current_a":     round(rng.uniform(0.1, 63.0), 3),
        "power_kw":      round(rng.uniform(0.05, 15.0), 3),
        "energy_kwh":    round(rng.uniform(0, 9999), 2),
        "power_factor":  round(rng.uniform(0.75, 1.0), 3),
        "frequency_hz":  round(rng.uniform(49.8, 50.2), 3),
        "phase":         rng.choice(["L1", "L2", "L3"]),
        "tariff_zone":   rng.choice(["peak", "off-peak", "shoulder"]),
        "alert":         rng.choice(ALERT_LEVELS),
        "ts":            _ts(),
    }


def gen_gps_tracker(rng: random.Random) -> dict:
    # Simulate movement around a city bounding box
    base_lat, base_lon = 19.076, 72.877   # Mumbai as example city
    return {
        "event_type":   "gps_tracker",
        "vehicle_id":   rng.choice(VEHICLE_IDS),
        "lat":          round(base_lat + rng.uniform(-0.15, 0.15), 6),
        "lon":          round(base_lon + rng.uniform(-0.15, 0.15), 6),
        "altitude_m":   round(rng.uniform(0, 120), 1),
        "speed_kmh":    round(rng.uniform(0, 120), 1),
        "heading_deg":  rng.randint(0, 359),
        "satellites":   rng.randint(4, 12),
        "hdop":         round(rng.uniform(0.8, 3.5), 2),
        "engine":       rng.choice(["on", "on", "on", "off"]),
        "fuel_pct":     rng.randint(5, 100),
        "odometer_km":  rng.randint(0, 250000),
        "event_tag":    rng.choice([None, None, None, "harsh_brake", "speeding", "geofence_exit"]),
        "ts":           _ts(),
    }


def gen_vibration(rng: random.Random) -> dict:
    mach = rng.choice(MACHINE_IDS)
    rms = round(rng.uniform(0.01, 15.0), 4)
    return {
        "event_type":   "vibration",
        "machine_id":   mach,
        "location":     rng.choice(["factory_floor_a", "factory_floor_b", "loading_dock"]),
        "axis":         rng.choice(["x", "y", "z", "composite"]),
        "rms_g":        rms,
        "peak_g":       round(rms * rng.uniform(1.2, 3.5), 4),
        "freq_hz":      round(rng.uniform(1, 1000), 2),
        "temp_bearing": round(rng.uniform(25, 110), 1),
        "rpm":          rng.randint(100, 3600),
        "health_score": rng.randint(0, 100),
        "alert":        "critical" if rms > 12 else ("warning" if rms > 8 else None),
        "ts":           _ts(),
    }


def gen_air_quality(rng: random.Random) -> dict:
    pm25 = round(rng.uniform(2, 350), 2)
    aqi = min(500, int(pm25 * 4))
    return {
        "event_type": "air_quality",
        "station_id": rng.choice(SENSOR_IDS),
        "location":   rng.choice(LOCATIONS),
        "pm2_5":      pm25,
        "pm10":       round(rng.uniform(5, 500), 2),
        "co2_ppm":    round(rng.uniform(400, 5000), 1),
        "co_ppm":     round(rng.uniform(0, 50), 3),
        "no2_ppb":    round(rng.uniform(0, 200), 2),
        "o3_ppb":     round(rng.uniform(0, 120), 2),
        "voc_ppb":    round(rng.uniform(0, 500), 1),
        "aqi":        aqi,
        "aqi_category": (
            "Good" if aqi < 51 else "Moderate" if aqi < 101 else
            "Unhealthy for Sensitive" if aqi < 151 else
            "Unhealthy" if aqi < 201 else "Very Unhealthy" if aqi < 301 else "Hazardous"
        ),
        "ts":         _ts(),
    }


# round-robin across generators for realistic mixed traffic
_GEN_FNS = [gen_env_sensor, gen_energy_meter, gen_gps_tracker, gen_vibration, gen_air_quality]


# ═══════════════════════════════════════════════════════════════════════════
#  WORKER PROCESS
# ═══════════════════════════════════════════════════════════════════════════

def _worker(
    queue: mp.Queue,
    stop_event: Any,
    worker_id: int,
    batch_size: int,
):
    """Runs in a child process. Generates events and puts batches onto the queue."""
    rng = random.Random(worker_id * 9999 + os.getpid())
    gen_cycle = itertools.cycle(_GEN_FNS)
    batch: list[str] = []

    while not stop_event.is_set():
        fn = next(gen_cycle)
        batch.append(json.dumps(fn(rng)))
        if len(batch) >= batch_size:
            queue.put(batch, block=True, timeout=1.0)
            batch = []

    # flush remaining
    if batch:
        try:
            queue.put(batch, block=False)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
#  CONSUMER / STATS THREAD
# ═══════════════════════════════════════════════════════════════════════════

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



class StreamConsumer:
    def __init__(self, queue, output_file, print_sample):
        self.queue = queue
        self.print_sample = print_sample
        self._count = 0
        self._last_count = 0
        self._last_time = time.perf_counter()
        self._running = True
        self._sample_buf: deque[str] = deque(maxlen=1)

    def _handle_event(self, raw: str):
        # 🔥 SEND TO KAFKA HERE
        try:
            event = json.loads(raw)
            producer.send("iot-events", event)
        except Exception as e:
            print("Kafka send error:", e)

        self._count += 1

    def run(self, duration: float):
        deadline = time.perf_counter() + duration
        stats_interval = 1.0
        next_stat = time.perf_counter() + stats_interval

        print(f"\n{'─'*60}")
        print(f"  IoT Stream Generator → Kafka")
        print(f"{'─'*60}\n")

        while time.perf_counter() < deadline and self._running:
            try:
                batch = self.queue.get(timeout=0.05)
                for raw in batch:
                    self._handle_event(raw)

                if self.print_sample and batch:
                    self._sample_buf.append(batch[-1])

            except Exception:
                pass

            now = time.perf_counter()
            if now >= next_stat:
                elapsed = now - self._last_time
                rate = (self._count - self._last_count) / elapsed
                self._last_count = self._count
                self._last_time = now
                next_stat += stats_interval

                print(f"{rate:,.0f} events/sec | total: {self._count:,}")

                if self.print_sample and self._sample_buf:
                    sample = json.loads(self._sample_buf[0])
                    print("sample:", sample.get("event_type"))

        producer.flush()
        print("\nTotal events:", self._count)

    def _flush(self):
        # drain remaining items
        while True:
            try:
                batch = self.queue.get_nowait()
                for raw in batch:
                    self._handle_event(raw)
            except Exception:
                break
        if self._fh:
            self._fh.close()

        elapsed_total = time.perf_counter()   # just a marker; we'll compute below
        print(f"\n{'─'*60}")
        print(f"  Total events generated : {self._count:,}")
        print(f"{'─'*60}\n")


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="High-performance IoT data stream generator (20k+ ev/s)"
    )
    parser.add_argument("--workers",    type=int,   default=max(1, mp.cpu_count() - 1),
                        help="Number of generator worker processes (default: CPU count - 1)")
    parser.add_argument("--batch",      type=int,   default=500,
                        help="Events per queue batch (default: 500)")
    parser.add_argument("--duration",   type=float, default=15.0,
                        help="How many seconds to run (default: 15)")
    parser.add_argument("--output",     type=str,   default=None,
                        help="Optional file path to write NDJSON events")
    parser.add_argument("--sample",     action="store_true",
                        help="Print a sample event each second")
    args = parser.parse_args()

    ctx        = mp.get_context("spawn")   # safe on all platforms
    queue      = ctx.Queue(maxsize=200)
    stop_event = ctx.Event()

    workers = []
    for wid in range(args.workers):
        p = ctx.Process(
            target=_worker,
            args=(queue, stop_event, wid, args.batch),
            daemon=True,
        )
        p.start()
        workers.append(p)

    consumer = StreamConsumer(queue, args.output, args.sample)

    def _sigint(sig, frame):
        consumer._running = False

    signal.signal(signal.SIGINT, _sigint)

    t0 = time.perf_counter()
    consumer.run(args.duration)
    elapsed = time.perf_counter() - t0

    stop_event.set()
    for p in workers:
        p.join(timeout=3)

    avg = consumer._count / elapsed if elapsed > 0 else 0
    print(f"  Average throughput : {avg:,.0f} events/sec over {elapsed:.1f}s")
    if avg >= 20_000:
        print("  \033[92m✔ Target of 20,000 ev/s achieved!\033[0m\n")
    else:
        tip = max(1, int(20_000 / (avg / args.workers + 1))) if avg > 0 else args.workers * 2
        print(f"  \033[93m⚠ Try --workers {tip} or --batch 1000 to hit the target.\033[0m\n")


if __name__ == "__main__":
    mp.freeze_support()
    main()
