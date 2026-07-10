#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 SAAT SIMULATION  â  Solar-Powered Automated Agricultural Technology
 Pear Sorting & Packaging System â ROS2 Production Workspace (SOFTWARE-ONLY SIM)
================================================================================

WHAT THIS FILE IS
------------------
This is a *pure-software simulation* of the SAAT ROS2 system described in the
project README and Vision Documentation. It does NOT touch any real hardware:

    - No RealSense camera is opened.
    - No Jetson.GPIO / PCA9685 / I2C / servo calls are made.
    - No PLC is contacted.

Instead, every node described in the README (18 node types / 29 node
instances) is re-implemented as a Python thread that talks to the others
through an in-process publish/subscribe "topic bus" that mimics ROS2 topics,
QoS (latched params), and the 4 custom message types (InfectionResult,
PearData, SpeedCommand, MotorStatus). Where the real system would read a
camera pixel or an encoder, this simulation draws a random number from a
distribution constrained by the exact thresholds/formulas in
`saat_params.yaml` (see CONFIG below) â e.g. infection_ratio is only ever
"REJECTED" when it lands >= 0.03, exactly like the real Otsu-based decision
rule in the 9-step vision pipeline.

WHAT YOU GET
------------
    1. A live console log of every node "publishing" (optional, --verbose).
    2. A SQLite database at ./saat_data/saat_records.db with the exact
       13-field schema from the README, written in the exact zone order
       A1 -> A2 -> A3 -> B1 -> B2 -> B3.
    3. A Flask "SCADA" web dashboard, replicating the 3 routes described in
       the README:
           http://localhost:8080            status page (auto-refresh 10s)
           http://localhost:8080/database    200 most-recent pear records
           http://localhost:8080/api/status  raw 13-field JSON IoT payload
       using the exact dark-IIoT color tokens from the README's "SCADA
       Dashboard" table.

HOW TO RUN
----------
    pip install flask
    python3 saat_simulation.py
    # then open http://localhost:8080 in a browser

USEFUL FLAGS
------------
    --port 8080          Port for the Flask dashboard (default 8080, matches README)
    --speed 1.0           Time-compression multiplier. 1.0 = "real" timing from the
                           README (1-second action cycle etc). 5.0 runs 5x faster so
                           you can see the database fill up quickly while testing.
    --reject-rate 0.30    Fraction of pears that come out infected (>=3% ratio).
    --seed 42              Random seed, for reproducible demo runs.
    --duration 0           Auto-stop after N seconds (0 = run until Ctrl+C).
    --no-web               Headless mode: run the node graph + DB writer only,
                           skip starting Flask (useful for quick sanity checks).
    --db-path PATH         Where to put the sqlite file
                           (default ./saat_data/saat_records.db)

IMPORTANT CAVEAT ON "EXACT VALUES"
-----------------------------------
Because there is no real camera and no real pears, the *numeric* values you
will see (infection_ratio, pear_mass_g, belt_speed_ms, etc.) are randomly
generated every run â the real system's numbers depend on actual images of
actual pears, which this simulation cannot know in advance. What IS exact
and reproducible is:
    - every field name, message schema, and database column (13/13, verbatim)
    - every threshold and formula from saat_params.yaml (3% infection cutoff,
      15,000 px^2 BIG/SMALL cutoff, Conv1+Conv2=3.3V constraint, 0.1V floor,
      0.96 g/cm^3 pear density, etc.)
    - the exact set of dashboard routes, JSON keys, and page layout/colors
    - the node graph, topic names, and message types, and the write order
So running this script reproduces the *behaviour and structure* of the real
system exactly; it cannot reproduce specific pear photos that don't exist.
================================================================================
"""

import argparse
import json
import queue
import random
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

try:
    from flask import Flask, jsonify, render_template_string
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False


# ==============================================================================
# COMPANY LOGO â embedded as base64 PNG so the simulation stays a single file.
# Used on the SCADA dashboard nav and on the printed package / pear labels.
# ==============================================================================
LOGO_B64_PNG = (
    "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"
    "SUVORK5CYII="
)


# Small (64x64) version for the 24 repeated per-pear labels â keeps page size sane.
LOGO_B64_PNG_SMALL = (
    "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"
    "rkJggg=="
)


# ==============================================================================
# CONFIG  â mirrors src/saat_core/config/saat_params.yaml, verbatim values
# ==============================================================================
CONFIG = {
    "frame_capture_node": {
        "frame_width": 1280,
        "frame_height": 720,
        "color_fps": 30,
    },
    "vision_init_node": {
        "infection_ratio_threshold": 0.03,   # 3%
        "max_depth_mm": 380,
        "hsv_lower": [10, 40, 40],
        "hsv_upper": [95, 255, 255],
    },
    "action_node": {
        "accept_angle": 0.0,
        "reject_angle": 90.0,
        "return_delay_s": 0.4,
    },
    "speed_publisher_node": {
        "gpio_pin_conv1": 11,
        "gpio_pin_conv2": 13,
        "pwm_frequency": 500,
        "min_voltage": 0.1,
        "max_voltage": 3.3,
        "kp_conv1": 1.0,
        "ki_conv1": 0.1,
        "kd_conv1": 0.05,
    },
    "mass_estimation_node": {
        "pear_density_g_cm3": 0.96,
    },
    "main_speed_node": {
        "px_to_m": 0.0005,
        "max_ref_speed_ms": 0.5,
    },
    "big_small_threshold_px2": 15000,     # PearData.pear_category cutoff
    "belt_lengths_cm": {"conv1": 175, "conv2": 115},
    "iot_publish_period_s": 10.0,          # 0.1 Hz, per README
    "packaging": {
        "big_per_package": 12,             # upper layer
        "small_per_package": 12,           # lower layer
        "company_name": "SAAT",
    },
}

ZONES = ["A1", "A2", "A3", "B1", "B2", "B3"]
ZONE_ORDER = ["A1", "A2", "A3", "B1", "B2", "B3"]  # DB write order, per README

# 9-step vision pipeline timing budget (ms), from README "1-Second Action Cycle"
PIPELINE_STEP_MS = {
    "clahe": 3, "bilateral": 8, "masks": 4,
    "morphology": 3, "otsu": 5, "contours": 4, "publish": 3,
}


# ==============================================================================
# CUSTOM MESSAGES  â saat_interfaces/*.msg, translated to dataclasses
# ==============================================================================
@dataclass
class InfectionResult:
    zone_id: str
    pear_detected: bool
    is_infected: bool
    infection_ratio: float
    infection_area_px: float
    pear_area_px: float
    infection_x: float
    infection_y: float
    infection_r: int
    infection_g: int
    infection_b: int
    pear_centroid_x: float
    pear_centroid_y: float
    stamp: float = field(default_factory=time.time)


@dataclass
class PearData:
    pear_id: str
    zone_id: str
    pear_status: str          # ACCEPTED | REJECTED
    pear_category: str        # SMALL | BIG
    infection_area_px: float
    infection_x: float
    infection_y: float
    infection_r: int
    infection_g: int
    infection_b: int
    infection_ratio: float
    pear_surface_area_px: float
    pear_volume_cm3: float
    pear_mass_g: float
    belt_speed_ms: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class SpeedCommand:
    reference_speed_ms: float
    conv1_voltage: float
    conv2_voltage: float
    servo_voltage: float
    belt_state: str            # EMPTY | NORMAL | CROWDED
    pear_count: int
    stamp: float = field(default_factory=time.time)


@dataclass
class MotorStatus:
    zone_ids: list
    servo_active: list
    last_action: list
    conv1_speed_ms: float
    conv2_speed_ms: float
    conv1_voltage: float
    conv2_voltage: float
    batch_accepted: int
    batch_rejected: int
    completed_packages: int
    stamp: float = field(default_factory=time.time)


@dataclass
class PackageLabel:
    """A completed package: 12 BIG pears (upper layer) + 12 SMALL pears (lower layer)."""
    package_id: str                # e.g. "PA00001"
    upper_layer: list              # [{"position":"P1","pear_id":...,"mass_g":...}, ...] x12 BIG
    lower_layer: list              # same shape, x12 SMALL
    upper_weight_g: float
    lower_weight_g: float
    total_weight_g: float
    start_time: float              # timestamp of the first pear collected into this package
    end_time: float                # timestamp the 24th pear completed the package
    duration_s: float              # "packaging clock" â time taken to fill the package


# ==============================================================================
# TOPIC BUS  â minimal in-process pub/sub, mimics ROS2 topics
# ==============================================================================
class Bus:
    """Latched-aware pub/sub bus. `latest()` mimics TRANSIENT_LOCAL QoS."""

    def __init__(self, verbose=False):
        self._subs = {}
        self._latest = {}
        self._lock = threading.Lock()
        self.verbose = verbose

    def subscribe(self, topic):
        q = queue.Queue()
        with self._lock:
            self._subs.setdefault(topic, []).append(q)
        return q

    def publish(self, topic, msg):
        with self._lock:
            self._latest[topic] = msg
            subs = list(self._subs.get(topic, []))
        for q in subs:
            q.put(msg)
        if self.verbose:
            print(f"[{time.strftime('%H:%M:%S')}] {topic} <- {msg}")

    def latest(self, topic, default=None):
        with self._lock:
            return self._latest.get(topic, default)


# ==============================================================================
# SHARED STATE  â counters that several nodes need to read/write
# ==============================================================================
class SharedState:
    def __init__(self):
        self._lock = threading.Lock()
        self._active = {z: False for z in ZONES}
        self._last_action = {z: "IDLE" for z in ZONES}
        self._pear_counter = {z: 0 for z in ZONES}
        self.batch_accepted = 0
        self.batch_rejected = 0
        self.completed_packages = 0

    def set_active(self, zone, val, action=None):
        with self._lock:
            self._active[zone] = val
            if action:
                self._last_action[zone] = action

    def active_zone_count(self):
        with self._lock:
            return sum(1 for v in self._active.values() if v)

    def motor_status_fields(self):
        with self._lock:
            return (list(ZONES),
                    [self._active[z] for z in ZONES],
                    [self._last_action[z] for z in ZONES])

    def next_pear_id(self, zone):
        with self._lock:
            self._pear_counter[zone] += 1
            return f"{zone}_{self._pear_counter[zone]:05d}"

    def record_result(self, status):
        with self._lock:
            if status == "ACCEPTED":
                self.batch_accepted += 1
            else:
                self.batch_rejected += 1

    def note_package_completed(self):
        with self._lock:
            self.completed_packages += 1


# ==============================================================================
# PACKAGING MANAGER  â collects ACCEPTED pears into 24-pear packages:
#   Upper layer = 12 BIG pears Â· Lower layer = 12 SMALL pears
# REJECTED pears are ejected off the line and never enter a package.
# Implemented with two FIFO queues so pears that arrive after a layer is
# already full simply wait for the *next* package (nothing is dropped).
# ==============================================================================
class PackagingManager:
    def __init__(self, big_count: int, small_count: int):
        self.big_count = big_count
        self.small_count = small_count
        self._lock = threading.Lock()
        self._big_q = deque()      # each item: (PearData, arrival_timestamp)
        self._small_q = deque()
        self.package_counter = 0

    def add_pear(self, pear: "PearData"):
        """Feed one ACCEPTED pear in. Returns a PackageLabel if this pear
        was the one that completed a package (12 BIG + 12 SMALL), else None."""
        if pear.pear_status != "ACCEPTED":
            return None  # rejected pears are ejected, never packaged

        now = time.time()
        with self._lock:
            if pear.pear_category == "BIG":
                self._big_q.append((pear, now))
            else:
                self._small_q.append((pear, now))

            if len(self._big_q) < self.big_count or len(self._small_q) < self.small_count:
                return None

            # Both layers are full -> pop the oldest 12 of each (FIFO) to form the package.
            big_items = [self._big_q.popleft() for _ in range(self.big_count)]
            small_items = [self._small_q.popleft() for _ in range(self.small_count)]
            self.package_counter += 1
            package_id = f"PA{self.package_counter:05d}"

            upper_layer = [
                {"position": f"P{i + 1}", "pear_id": p.pear_id, "mass_g": p.pear_mass_g}
                for i, (p, _ts) in enumerate(big_items)
            ]
            lower_layer = [
                {"position": f"P{i + 1}", "pear_id": p.pear_id, "mass_g": p.pear_mass_g}
                for i, (p, _ts) in enumerate(small_items)
            ]
            upper_weight_g = round(sum(p.pear_mass_g for p, _ in big_items), 2)
            lower_weight_g = round(sum(p.pear_mass_g for p, _ in small_items), 2)
            start_time = min(big_items[0][1], small_items[0][1])
            end_time = now

            return PackageLabel(
                package_id=package_id,
                upper_layer=upper_layer, lower_layer=lower_layer,
                upper_weight_g=upper_weight_g, lower_weight_g=lower_weight_g,
                total_weight_g=round(upper_weight_g + lower_weight_g, 2),
                start_time=start_time, end_time=end_time,
                duration_s=round(end_time - start_time, 2),
            )


# ==============================================================================
# DATABASE  â 13-field pear schema (verbatim from README) + packages/labels
# ==============================================================================
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS pear_records (
    pear_id               TEXT PRIMARY KEY,
    zone_id               TEXT,
    timestamp             REAL,
    pear_status           TEXT,
    pear_category         TEXT,
    infection_area_px     REAL,
    infection_location    TEXT,
    infection_color_rgb   TEXT,
    infection_ratio       REAL,
    pear_surface_area_px  REAL,
    pear_volume_cm3       REAL,
    pear_mass_g           REAL,
    belt_speed_ms         REAL
);

CREATE TABLE IF NOT EXISTS packages (
    package_id       TEXT PRIMARY KEY,
    start_timestamp  REAL,
    end_timestamp    REAL,
    duration_s       REAL,
    upper_layer      TEXT,   -- JSON list of 12 {position, pear_id, mass_g} (BIG)
    lower_layer      TEXT,   -- JSON list of 12 {position, pear_id, mass_g} (SMALL)
    upper_weight_g   REAL,
    lower_weight_g   REAL,
    total_weight_g   REAL
);
"""


def init_db(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.executescript(DB_SCHEMA)
    conn.commit()
    return conn


# ==============================================================================
# NODE: vision_init_node  (LAYER 0 â runs once, broadcasts latched /vision_params)
# ==============================================================================
def vision_init_node(bus: Bus):
    payload = json.dumps(CONFIG["vision_init_node"])
    bus.publish("/vision_params", payload)
    print("[vision_init_node] classical_vision_initialization complete.")


# ==============================================================================
# NODE: frame_capture_node  (LAYER 1 â simulated RealSense D455)
# ==============================================================================
def frame_capture_node(bus: Bus, stop_event: threading.Event, speed: float):
    fps = CONFIG["frame_capture_node"]["color_fps"]
    period = (1.0 / fps) / speed
    frame_id = 0
    print(f"[frame_capture_node] RealSense D455 started (simulated): "
          f"{CONFIG['frame_capture_node']['frame_width']}x"
          f"{CONFIG['frame_capture_node']['frame_height']} colour@{fps}fps depth@{fps}fps")
    while not stop_event.is_set():
        frame_id += 1
        bus.publish("/raw_frame", frame_id)
        bus.publish("/raw_depth", frame_id)
        time.sleep(period)


# ==============================================================================
# NODES: <zone>_vision + <zone>_action + <zone>_area_node + <zone>_data_collector
#         (LAYERS 3+4, combined per-zone pipeline thread, x6)
# ==============================================================================
def zone_pipeline(zone: str, bus: Bus, state: SharedState, stop_event: threading.Event,
                   speed: float, reject_rate: float, rng: random.Random):
    threshold = CONFIG["vision_init_node"]["infection_ratio_threshold"]
    big_small_cut = CONFIG["big_small_threshold_px2"]
    density = CONFIG["mass_estimation_node"]["pear_density_g_cm3"]
    return_delay = CONFIG["action_node"]["return_delay_s"]
    pipeline_s = sum(PIPELINE_STEP_MS.values()) / 1000.0

    # simulated belt inter-arrival time for a pear reaching this zone (seconds)
    arrival_lo, arrival_hi = 1.2, 3.0

    while not stop_event.is_set():
        wait = rng.uniform(arrival_lo, arrival_hi) / speed
        slept = 0.0
        while slept < wait and not stop_event.is_set():
            step = min(0.2, wait - slept)
            time.sleep(step)
            slept += step
        if stop_event.is_set():
            break

        state.set_active(zone, True)
        pear_id = state.next_pear_id(zone)

        # --- Step 1-9: 9-step vision pipeline (simulated timing + result) -----
        time.sleep(pipeline_s / speed)

        is_big = rng.random() < 0.5
        pear_area_px = rng.uniform(big_small_cut + 500, big_small_cut + 11000) if is_big \
            else rng.uniform(big_small_cut - 7000, big_small_cut - 500)
        pear_category = "BIG" if pear_area_px >= big_small_cut else "SMALL"

        is_infected = rng.random() < reject_rate
        infection_ratio = rng.uniform(threshold, threshold + 0.15) if is_infected \
            else rng.uniform(0.0, threshold - 0.001)
        infection_area_px = infection_ratio * pear_area_px

        pear_cx, pear_cy = rng.uniform(30, 397), rng.uniform(30, 330)
        infection_x = min(max(pear_cx + rng.uniform(-25, 25), 0), 427)
        infection_y = min(max(pear_cy + rng.uniform(-25, 25), 0), 360)
        # dominant colour of infection region: brown/black rot tones
        r, g, b = rng.randint(35, 95), rng.randint(20, 60), rng.randint(10, 40)

        detection = InfectionResult(
            zone_id=zone, pear_detected=True, is_infected=is_infected,
            infection_ratio=round(infection_ratio, 4),
            infection_area_px=round(infection_area_px, 1),
            pear_area_px=round(pear_area_px, 1),
            infection_x=round(infection_x, 1), infection_y=round(infection_y, 1),
            infection_r=r, infection_g=g, infection_b=b,
            pear_centroid_x=round(pear_cx, 1), pear_centroid_y=round(pear_cy, 1),
        )
        bus.publish(f"/{zone}/detection", detection)

        # --- action_node: classification decision + simulated servo motion ---
        status = "REJECTED" if is_infected else "ACCEPTED"
        bus.publish(f"/{zone}/action", status)
        state.set_active(zone, True, action=status)
        # servo moves to reject/accept position, then returns (simulated MG995 timing)
        time.sleep((return_delay + return_delay) / speed)

        # --- area_node -------------------------------------------------------
        bus.publish(f"/{zone}/area", detection.pear_area_px)

        # --- volume_estimation_node (depth -> cm^3) --------------------------
        # simulated depth-derived volume: realistic pear range ~80-260 cm^3
        volume_cm3 = round(pear_area_px * rng.uniform(0.0065, 0.0090), 2)
        bus.publish(f"/{zone}/volume", volume_cm3)

        # --- mass_estimation_node (volume * density) --------------------------
        mass_g = round(volume_cm3 * density, 2)
        bus.publish(f"/{zone}/mass", mass_g)

        # --- data_collector: assemble PearData -------------------------------
        belt_speed = bus.latest("/speed_to_plc")
        belt_speed_ms = belt_speed.reference_speed_ms if belt_speed else 0.0
        pear_data = PearData(
            pear_id=pear_id, zone_id=zone, pear_status=status, pear_category=pear_category,
            infection_area_px=detection.infection_area_px,
            infection_x=detection.infection_x, infection_y=detection.infection_y,
            infection_r=r, infection_g=g, infection_b=b,
            infection_ratio=detection.infection_ratio,
            pear_surface_area_px=detection.pear_area_px,
            pear_volume_cm3=volume_cm3, pear_mass_g=mass_g,
            belt_speed_ms=round(belt_speed_ms, 4),
        )
        bus.publish(f"/{zone}/pear_data", pear_data)

        state.set_active(zone, False, action="IDLE")
        state.record_result(status)


# ==============================================================================
# NODE: infection_description_node  (LAYER 5 â aggregates all 6 zones -> JSON)
# ==============================================================================
def infection_description_node(bus: Bus, stop_event: threading.Event, speed: float):
    period = 1.0 / speed
    while not stop_event.is_set():
        agg = {}
        for z in ZONES:
            det = bus.latest(f"/{z}/detection")
            agg[z] = asdict(det) if det else None
        bus.publish("/infection_description", json.dumps(agg, default=str))
        time.sleep(period)


# ==============================================================================
# NODE: main_speed_node  (LAYER 6 â 7-step centroid tracking speed reference)
# ==============================================================================
def main_speed_node(bus: Bus, state: SharedState, stop_event: threading.Event,
                     speed: float, rng: random.Random):
    max_speed = CONFIG["main_speed_node"]["max_ref_speed_ms"]
    min_v, max_v = CONFIG["speed_publisher_node"]["min_voltage"], CONFIG["speed_publisher_node"]["max_voltage"]
    period = 0.5 / speed

    while not stop_event.is_set():
        # Step 1-2: detect + count pears in vision zone (active zones + rare "queued overflow")
        active = state.active_zone_count()
        overflow = rng.randint(1, 3) if rng.random() < 0.05 else 0
        pear_count = active + overflow

        if pear_count == 0:
            belt_state = "EMPTY"
            conv1_v, conv2_v, ref_speed = max_v, min_v, 0.0
        elif pear_count > 6:
            belt_state = "CROWDED"
            conv1_v, conv2_v, ref_speed = min_v, max_v, max_speed
        else:
            belt_state = "NORMAL"
            # Steps 3-6: simulated per-pear centroid displacement -> average speed
            ref_speed = rng.uniform(0.05, max_speed)
            # Step 7: map speed to voltage, enforce Conv1+Conv2 = 3.3V
            conv2_v = (ref_speed / max_speed) * (max_v)
            conv1_v = max_v - conv2_v
            conv1_v = min(max(conv1_v, min_v), max_v)
            conv2_v = min(max(conv2_v, min_v), max_v)

        servo_voltage = round(min(max(ref_speed * 6.0, 0.0), 5.0), 3)

        cmd = SpeedCommand(
            reference_speed_ms=round(ref_speed, 4),
            conv1_voltage=round(conv1_v, 3),
            conv2_voltage=round(conv2_v, 3),
            servo_voltage=servo_voltage,
            belt_state=belt_state,
            pear_count=pear_count,
        )
        bus.publish("/main_speed", cmd)
        bus.publish("/centroid_time_speed", ref_speed)
        time.sleep(period)


# ==============================================================================
# NODES: conv1_speed_node / conv2_speed_node  (simulated optical-flow feedback)
# ==============================================================================
def conv_speed_node(channel: str, bus: Bus, stop_event: threading.Event,
                     speed: float, rng: random.Random):
    max_v = CONFIG["speed_publisher_node"]["max_voltage"]
    max_speed = CONFIG["main_speed_node"]["max_ref_speed_ms"]
    period = 0.1 / speed
    topic_out = f"/conv{channel}_speed_feedback"
    while not stop_event.is_set():
        cmd = bus.latest("/main_speed")
        target_v = (cmd.conv1_voltage if channel == "1" else cmd.conv2_voltage) if cmd else 0.1
        # invert voltage -> speed mapping, add sensor noise (simulated optical flow)
        est_speed = (target_v / max_v) * max_speed
        est_speed = max(0.0, est_speed + rng.gauss(0, 0.01))
        bus.publish(topic_out, round(est_speed, 4))
        time.sleep(period)


# ==============================================================================
# NODE: servo_speed_node  (PID for servo sweep speed)
# ==============================================================================
def servo_speed_node(bus: Bus, stop_event: threading.Event, speed: float, rng: random.Random):
    period = 0.1 / speed
    while not stop_event.is_set():
        cmd = bus.latest("/main_speed")
        target = cmd.servo_voltage if cmd else 0.0
        servo_cmd = max(0.0, target + rng.gauss(0, 0.02))
        bus.publish("/servo_cmd", round(servo_cmd, 3))
        time.sleep(period)


class PID:
    """Textbook discrete PID, gains taken verbatim from saat_params.yaml."""

    def __init__(self, kp, ki, kd):
        self.kp, self.ki, self.kd = kp, ki, kd
        self.integral = 0.0
        self.prev_error = 0.0

    def step(self, setpoint, measurement, dt):
        error = setpoint - measurement
        self.integral += error * dt
        derivative = (error - self.prev_error) / dt if dt > 0 else 0.0
        self.prev_error = error
        return self.kp * error + self.ki * self.integral + self.kd * derivative


# ==============================================================================
# NODE: speed_publisher_node  (LAYER 6 â dual-channel PID, runs at 10Hz,
#                               enforces Conv1_V + Conv2_V = 3.3V)
# ==============================================================================
def speed_publisher_node(bus: Bus, stop_event: threading.Event, speed: float):
    cfg = CONFIG["speed_publisher_node"]
    min_v, max_v = cfg["min_voltage"], cfg["max_voltage"]
    max_speed = CONFIG["main_speed_node"]["max_ref_speed_ms"]
    pid1 = PID(cfg["kp_conv1"], cfg["ki_conv1"], cfg["kd_conv1"])
    dt = 0.1 / speed  # 10 Hz PID loop, per README

    while not stop_event.is_set():
        cmd = bus.latest("/main_speed")
        fb1 = bus.latest("/conv1_speed_feedback", 0.0)
        if cmd is None:
            time.sleep(dt)
            continue

        target_speed1 = (cmd.conv1_voltage / max_v) * max_speed
        correction = pid1.step(target_speed1, fb1, dt)
        conv1_v = cmd.conv1_voltage + correction * 0.05  # small PID trim on top of setpoint
        conv1_v = min(max(conv1_v, min_v), max_v)
        conv2_v = min(max(max_v - conv1_v, min_v), max_v)  # constraint enforced HERE
        conv1_v = max_v - conv2_v                          # re-derive to guarantee exact sum

        final = SpeedCommand(
            reference_speed_ms=cmd.reference_speed_ms,
            conv1_voltage=round(conv1_v, 3),
            conv2_voltage=round(conv2_v, 3),
            servo_voltage=cmd.servo_voltage,
            belt_state=cmd.belt_state,
            pear_count=cmd.pear_count,
        )
        bus.publish("/speed_to_plc", final)
        # GPIO 11 / GPIO 13 PWM duty cycle would be written here on real hardware.
        time.sleep(dt)


# ==============================================================================
# NODE: data_collection_node  (LAYER 7 â SQLite writer, ordered A1->B3, + IoT pub)
# ==============================================================================
def data_collection_node(bus: Bus, state: SharedState, packaging: "PackagingManager",
                          conn: sqlite3.Connection, stop_event: threading.Event, speed: float):
    queues = {z: bus.subscribe(f"/{z}/pear_data") for z in ZONE_ORDER}
    iot_period = CONFIG["iot_publish_period_s"] / speed
    last_iot = 0.0
    last_record = None

    while not stop_event.is_set():
        wrote_any = False
        for z in ZONE_ORDER:
            try:
                pear: PearData = queues[z].get_nowait()
            except queue.Empty:
                continue
            wrote_any = True
            conn.execute(
                """INSERT OR REPLACE INTO pear_records
                   (pear_id, zone_id, timestamp, pear_status, pear_category,
                    infection_area_px, infection_location, infection_color_rgb,
                    infection_ratio, pear_surface_area_px, pear_volume_cm3,
                    pear_mass_g, belt_speed_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    pear.pear_id, pear.zone_id, pear.timestamp, pear.pear_status,
                    pear.pear_category, pear.infection_area_px,
                    json.dumps({"x": pear.infection_x, "y": pear.infection_y}),
                    json.dumps([pear.infection_r, pear.infection_g, pear.infection_b]),
                    pear.infection_ratio, pear.pear_surface_area_px,
                    pear.pear_volume_cm3, pear.pear_mass_g, pear.belt_speed_ms,
                ),
            )
            conn.commit()
            last_record = pear
            print(f"[data_collection_node] wrote {pear.pear_id} "
                  f"({pear.zone_id}) {pear.pear_status}/{pear.pear_category} "
                  f"ratio={pear.infection_ratio:.3f} mass={pear.pear_mass_g:.1f}g")

            # --- packaging_node (logically part of Layer 7): feed the pear in ---
            label = packaging.add_pear(pear)
            if label is not None:
                conn.execute(
                    """INSERT OR REPLACE INTO packages
                       (package_id, start_timestamp, end_timestamp, duration_s,
                        upper_layer, lower_layer, upper_weight_g, lower_weight_g,
                        total_weight_g)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        label.package_id, label.start_time, label.end_time, label.duration_s,
                        json.dumps(label.upper_layer), json.dumps(label.lower_layer),
                        label.upper_weight_g, label.lower_weight_g, label.total_weight_g,
                    ),
                )
                conn.commit()
                state.note_package_completed()
                bus.publish("/package_completed", asdict(label))
                print(f"[data_collection_node] PACKAGE COMPLETE: {label.package_id} "
                      f"total={label.total_weight_g}g duration={label.duration_s}s "
                      f"-> label printed at /labels/{label.package_id}")

        now = time.time()
        if now - last_iot >= iot_period:
            last_iot = now
            speed_cmd = bus.latest("/speed_to_plc")
            zone_ids, servo_active, last_action = state.motor_status_fields()
            iot_payload = {
                "last_pear": asdict(last_record) if last_record else None,
                "belt": {
                    "conv1_voltage": speed_cmd.conv1_voltage if speed_cmd else None,
                    "conv2_voltage": speed_cmd.conv2_voltage if speed_cmd else None,
                    "reference_speed_ms": speed_cmd.reference_speed_ms if speed_cmd else None,
                    "belt_state": speed_cmd.belt_state if speed_cmd else None,
                    "pear_count": speed_cmd.pear_count if speed_cmd else None,
                },
                "zones": {z: a for z, a in zip(zone_ids, last_action)},
                "servo_active": {z: a for z, a in zip(zone_ids, servo_active)},
                "batch_accepted": state.batch_accepted,
                "batch_rejected": state.batch_rejected,
                "completed_packages": state.completed_packages,
                "timestamp": now,
            }
            bus.publish("/iot_status", json.dumps(iot_payload))

        if not wrote_any:
            time.sleep(0.05)


# ==============================================================================
# NODE: webpage_publisher_node  (LAYER 7 â Flask SCADA dashboard on :8080)
# ==============================================================================
COLORS = {
    "bg": "#0d1117", "surface": "#161b22", "border": "#30363d",
    "green": "#00ff88", "amber": "#f59e0b", "red": "#ef4444", "blue": "#3b82f6",
}

STATUS_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="10">
<title>SAAT SCADA â Status</title>
<style>
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  h1{color:{{c.green}};font-size:20px;letter-spacing:1px;}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-top:16px;}
  .card{background:{{c.surface}};border:1px solid {{c.border}};border-radius:8px;padding:16px;}
  .card h2{margin:0 0 8px 0;font-size:13px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;}
  .val{font-size:26px;font-weight:bold;}
  .accepted{color:{{c.green}};} .rejected{color:{{c.red}};}
  .amber{color:{{c.amber}};} .blue{color:{{c.blue}};}
  table{width:100%;border-collapse:collapse;margin-top:8px;font-size:13px;}
  td,th{border-bottom:1px solid {{c.border}};padding:4px 8px;text-align:left;}
  a{color:{{c.blue}};text-decoration:none;}
  .footer{margin-top:24px;color:#484f58;font-size:11px;}
</style></head><body>
<h1>&#9679; SAAT SCADA DASHBOARD </h1>
<div class="grid">
  <div class="card"><h2>Belt State</h2><div class="val {{ 'accepted' if belt.belt_state=='NORMAL' else ('amber' if belt.belt_state=='EMPTY' else 'rejected') }}">{{ belt.belt_state or 'â' }}</div></div>
  <div class="card"><h2>Conv1 Voltage</h2><div class="val blue">{{ '%.3f'|format(belt.conv1_voltage or 0) }} V</div></div>
  <div class="card"><h2>Conv2 Voltage</h2><div class="val blue">{{ '%.3f'|format(belt.conv2_voltage or 0) }} V</div></div>
  <div class="card"><h2>Reference Speed</h2><div class="val">{{ '%.4f'|format(belt.reference_speed_ms or 0) }} m/s</div></div>
  <div class="card"><h2>Pear Count (Vision Zone)</h2><div class="val">{{ belt.pear_count or 0 }}</div></div>
  <div class="card"><h2>Accepted</h2><div class="val accepted">{{ batch_accepted }}</div></div>
  <div class="card"><h2>Rejected</h2><div class="val rejected">{{ batch_rejected }}</div></div>
  <div class="card"><h2>Completed Packages</h2><div class="val amber"><a href="/labels" style="color:inherit;">{{ completed_packages }}</a></div></div>
</div>

<div class="card" style="margin-top:16px;">
  <h2>Zone Status (last action)</h2>
  <table><tr>{% for z in zones %}<th>{{z}}</th>{% endfor %}</tr>
  <tr>{% for z in zones %}<td class="{{ 'accepted' if zones[z]=='ACCEPTED' else ('rejected' if zones[z]=='REJECTED' else '') }}">{{ zones[z] }}</td>{% endfor %}</tr></table>
</div>

<div class="card" style="margin-top:16px;">
  <h2>Last Pear Record</h2>
  {% if last_pear %}
  <table>
    {% for k,v in last_pear.items() %}<tr><td>{{k}}</td><td>{{v}}</td></tr>{% endfor %}
  </table>
  {% else %}<div>No pears processed yetâ¦</div>{% endif %}
</div>

<div class="footer">
  <a href="/database">/database</a> &nbsp;|&nbsp; <a href="/labels">/labels</a> &nbsp;|&nbsp;
  <a href="/api/status">/api/status</a>
  &nbsp;|&nbsp; auto-refresh every 10s &nbsp;|&nbsp; updated {{ now }}
</div>
</body></html>
"""

DATABASE_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT SCADA â Database</title>
<style>
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  h1{color:{{c.green}};font-size:20px;}
  table{width:100%;border-collapse:collapse;font-size:12px;margin-top:16px;}
  td,th{border-bottom:1px solid {{c.border}};padding:4px 8px;text-align:left;white-space:nowrap;}
  th{color:#8b949e;text-transform:uppercase;font-size:11px;}
  .ACCEPTED{color:{{c.green}};} .REJECTED{color:{{c.red}};}
  a{color:{{c.blue}};}
</style></head><body>
<h1>&#128190; pear_records â 200 most recent</h1>
<p><a href="/">&larr; back to status</a></p>
<table><tr>{% for col in columns %}<th>{{col}}</th>{% endfor %}</tr>
{% for row in rows %}<tr>{% for i,val in enumerate(row) %}
<td class="{{ row[3] if i==3 else '' }}">{{ val }}</td>{% endfor %}</tr>{% endfor %}
</table>
</body></html>
"""


LABELS_INDEX_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT SCADA â Package Labels</title>
<style>
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  h1{color:{{c.green}};font-size:20px;}
  table{width:100%;border-collapse:collapse;font-size:13px;margin-top:16px;}
  td,th{border-bottom:1px solid {{c.border}};padding:6px 10px;text-align:left;}
  th{color:#8b949e;text-transform:uppercase;font-size:11px;}
  a{color:{{c.blue}};text-decoration:none;}
  a:hover{text-decoration:underline;}
  .empty{color:#8b949e;margin-top:16px;}
</style></head><body>
<h1>&#127991; Package Labels â completed packages (12 BIG + 12 SMALL each)</h1>
<p><a href="/">&larr; back to status</a></p>
{% if packages %}
<table>
<tr><th>Package ID</th><th>Completed At</th><th>Packaging Clock</th><th>Upper (BIG) g</th><th>Lower (SMALL) g</th><th>Total g</th><th></th></tr>
{% for p in packages %}
<tr>
  <td>{{p.package_id}}</td>
  <td>{{p.completed_at}}</td>
  <td>{{p.duration}}</td>
  <td>{{p.upper_weight_g}}</td>
  <td>{{p.lower_weight_g}}</td>
  <td>{{p.total_weight_g}}</td>
  <td><a href="/labels/{{p.package_id}}">print label &rarr;</a></td>
</tr>
{% endfor %}
</table>
{% else %}
<div class="empty">No packages completed yet â each package needs 12 BIG + 12 SMALL accepted pears.</div>
{% endif %}
</body></html>
"""

LABEL_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>Label â {{package_id}}</title>
<style>
  body{background:#e9edf1;color:#111;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  .backlink{display:block;margin-bottom:16px;color:#3b82f6;text-decoration:none;font-size:13px;}
  .label-master{background:#fff;border:3px solid #111;width:980px;margin:0 auto 32px auto;}
  .mh-top{border-bottom:3px solid #111;padding:10px 16px;display:flex;align-items:center;justify-content:flex-end;gap:10px;}
  .mh-top img{height:42px;}
  .mh-top span{font-size:22px;font-weight:bold;letter-spacing:2px;}
  .mh-body{display:grid;grid-template-columns:1fr 1fr 260px;}
  .layer-col{padding:16px;border-right:2px solid #111;}
  .info-col{border-right:none;padding:16px;display:flex;flex-direction:column;gap:14px;align-items:center;}
  .layer-title{text-align:center;font-weight:bold;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;}
  .pear-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:4px;}
  .pear-cell{border:1px solid #111;padding:8px 2px;text-align:center;font-size:11px;line-height:1.4;}
  .pear-cell b{display:block;font-size:12px;}
  .layer-total{text-align:center;margin-top:10px;font-weight:bold;font-size:13px;}
  .info-col img.round{width:64px;height:64px;border-radius:50%;object-fit:contain;border:2px solid #111;padding:4px;background:#fff;}
  .info-row{width:100%;text-align:center;}
  .info-row .lbl{display:block;font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;}
  .info-row .val{display:block;font-size:14px;font-weight:bold;margin-top:2px;}

  h2.section{width:980px;margin:24px auto 8px auto;font-size:15px;color:#333;}
  .pear-labels-grid{width:980px;margin:0 auto;display:flex;flex-wrap:wrap;gap:10px;}
  .pear-label{display:flex;width:230px;height:78px;border:2px solid #111;background:#fff;}
  .pear-label .logo-box{width:70px;flex-shrink:0;display:flex;align-items:center;justify-content:center;border-right:2px solid #111;}
  .pear-label .logo-box img{width:48px;height:48px;border-radius:50%;object-fit:contain;}
  .pear-label .info{flex:1;padding:6px 8px;display:flex;flex-direction:column;justify-content:center;font-size:11px;gap:4px;}
  .pear-label .info b{font-size:12px;}
  .cat-BIG{border-left:6px solid #3b82f6;}
  .cat-SMALL{border-left:6px solid #f59e0b;}
</style></head><body>
<a class="backlink" href="/labels">&larr; back to package list</a>

<div class="label-master">
  <div class="mh-top">
    <img src="data:image/png;base64,{{logo}}"><span>SAAT</span>
  </div>
  <div class="mh-body">
    <div class="layer-col">
      <div class="layer-title">Upper Layer (BIG)</div>
      <div class="pear-grid">
        {% for cell in upper %}<div class="pear-cell">{{cell.position}}<b>{{cell.pear_id}}</b></div>{% endfor %}
      </div>
      <div class="layer-total">Total Mass: {{upper_weight_g}} g</div>
    </div>
    <div class="layer-col">
      <div class="layer-title">Lower Layer (SMALL)</div>
      <div class="pear-grid">
        {% for cell in lower %}<div class="pear-cell">{{cell.position}}<b>{{cell.pear_id}}</b></div>{% endfor %}
      </div>
      <div class="layer-total">Total Mass: {{lower_weight_g}} g</div>
    </div>
    <div class="info-col">
      <img class="round" src="data:image/png;base64,{{logo}}">
      <div class="info-row"><span class="lbl">Package ID</span><span class="val">{{package_id}}</span></div>
      <div class="info-row"><span class="lbl">Packaging Time</span><span class="val">{{packaging_time}}</span></div>
      <div class="info-row"><span class="lbl">Packaging Clock</span><span class="val">{{packaging_clock}}</span></div>
      <div class="info-row"><span class="lbl">Total Weight</span><span class="val">{{total_weight_g}} g</span></div>
    </div>
  </div>
</div>

<h2 class="section">Individual Pear Labels ({{all_pears|length}})</h2>
<div class="pear-labels-grid">
  {% for item in all_pears %}
  <div class="pear-label cat-{{item.category}}">
    <div class="logo-box"><img src="data:image/png;base64,{{logo_small}}"></div>
    <div class="info">
      <div>P: <b>{{package_id}}</b></div>
      <div>PearID: <b>{{item.pear_id}}</b></div>
    </div>
  </div>
  {% endfor %}
</div>
</body></html>
"""


def build_flask_app(bus: Bus, db_path: Path):
    app = Flask(__name__)

    @app.route("/")
    def status():
        raw = bus.latest("/iot_status")
        payload = json.loads(raw) if raw else {
            "belt": {}, "zones": {z: "IDLE" for z in ZONES},
            "batch_accepted": 0, "batch_rejected": 0, "completed_packages": 0,
            "last_pear": None,
        }
        return render_template_string(
            STATUS_PAGE, c=COLORS, belt=payload.get("belt", {}) or {},
            zones=payload.get("zones", {}), batch_accepted=payload.get("batch_accepted", 0),
            batch_rejected=payload.get("batch_rejected", 0),
            completed_packages=payload.get("completed_packages", 0),
            last_pear=payload.get("last_pear"),
            now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    @app.route("/database")
    def database():
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute(
            "SELECT * FROM pear_records ORDER BY timestamp DESC LIMIT 200")
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
        conn.close()
        return render_template_string(
            DATABASE_PAGE, c=COLORS, columns=columns, rows=rows, enumerate=enumerate)

    @app.route("/api/status")
    def api_status():
        raw = bus.latest("/iot_status")
        return jsonify(json.loads(raw) if raw else {})

    @app.route("/labels")
    def labels_index():
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute(
            "SELECT package_id, end_timestamp, duration_s, upper_weight_g, "
            "lower_weight_g, total_weight_g FROM packages ORDER BY end_timestamp DESC")
        rows = cur.fetchall()
        conn.close()
        packages = [{
            "package_id": r[0],
            "completed_at": datetime.fromtimestamp(r[1]).strftime("%Y-%m-%d %H:%M:%S"),
            "duration": f"{r[2]:.1f}s",
            "upper_weight_g": r[3], "lower_weight_g": r[4], "total_weight_g": r[5],
        } for r in rows]
        return render_template_string(LABELS_INDEX_PAGE, c=COLORS, packages=packages)

    @app.route("/labels/<package_id>")
    def label_detail(package_id):
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute(
            "SELECT package_id, start_timestamp, end_timestamp, duration_s, "
            "upper_layer, lower_layer, upper_weight_g, lower_weight_g, total_weight_g "
            "FROM packages WHERE package_id = ?", (package_id,))
        row = cur.fetchone()
        conn.close()
        if row is None:
            return f"Package {package_id} not found. <a href='/labels'>Back</a>", 404

        (_pid, start_ts, end_ts, duration_s, upper_json, lower_json,
         upper_w, lower_w, total_w) = row
        upper = json.loads(upper_json)
        lower = json.loads(lower_json)
        all_pears = (
            [{"pear_id": c["pear_id"], "category": "BIG"} for c in upper] +
            [{"pear_id": c["pear_id"], "category": "SMALL"} for c in lower]
        )
        mins, secs = divmod(int(duration_s), 60)
        packaging_clock = f"{mins:02d}:{secs:02d}"

        return render_template_string(
            LABEL_PAGE, logo=LOGO_B64_PNG, logo_small=LOGO_B64_PNG_SMALL, package_id=package_id,
            upper=upper, lower=lower, upper_weight_g=upper_w, lower_weight_g=lower_w,
            total_weight_g=total_w, all_pears=all_pears,
            packaging_time=datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S"),
            packaging_clock=packaging_clock,
        )

    return app


# ==============================================================================
# LAUNCH  â mirrors saat_launch.py's staggered T+0 ... T+6s startup
# ==============================================================================
def main():
    ap = argparse.ArgumentParser(description="SAAT ROS2 system â software-only simulation")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--speed", type=float, default=1.0,
                     help="time-compression multiplier (1.0 = real README timing)")
    ap.add_argument("--reject-rate", type=float, default=0.30)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--duration", type=float, default=0.0,
                     help="auto-stop after N seconds (0 = run until Ctrl+C)")
    ap.add_argument("--no-web", action="store_true", help="headless: skip Flask dashboard")
    ap.add_argument("--db-path", type=str, default="./saat_data/saat_records.db")
    ap.add_argument("--verbose", action="store_true", help="print every bus message")
    args = ap.parse_args()

    if not FLASK_AVAILABLE and not args.no_web:
        print("Flask is not installed. Install it with:\n    pip install flask\n"
              "or re-run with --no-web to run headless.")
        return

    rng = random.Random(args.seed)
    bus = Bus(verbose=args.verbose)
    state = SharedState()
    stop_event = threading.Event()
    db_path = Path(args.db_path)
    conn = init_db(db_path)
    packaging = PackagingManager(
        big_count=CONFIG["packaging"]["big_per_package"],
        small_count=CONFIG["packaging"]["small_per_package"],
    )

    print("=" * 70)
    print(" SAAT SIMULATION â starting staggered node launch (like saat_launch.py)")
    print("=" * 70)

    threads = []

    # T+0
    print("[SAAT] T+0: Starting vision_init_node...")
    vision_init_node(bus)
    time.sleep(0.3 / args.speed)

    # T+1
    print("[SAAT] T+1: Starting RealSense capture (simulated)...")
    t = threading.Thread(target=frame_capture_node, args=(bus, stop_event, args.speed), daemon=True)
    t.start(); threads.append(t)
    time.sleep(0.3 / args.speed)

    # T+2/T+3: zone pipelines (frame_divider + vision + action + area + data_collector, x6)
    print("[SAAT] T+2/3: Starting 6 zone pipelines (vision + action + area + data_collector)...")
    for z in ZONES:
        t = threading.Thread(target=zone_pipeline,
                              args=(z, bus, state, stop_event, args.speed, args.reject_rate, rng),
                              daemon=True)
        t.start(); threads.append(t)
    time.sleep(0.3 / args.speed)

    # T+4: aggregators + speed pipeline
    print("[SAAT] T+4: Starting infection_description_node + speed pipeline...")
    for target, targs in [
        (infection_description_node, (bus, stop_event, args.speed)),
        (main_speed_node, (bus, state, stop_event, args.speed, rng)),
        (conv_speed_node, ("1", bus, stop_event, args.speed, rng)),
        (conv_speed_node, ("2", bus, stop_event, args.speed, rng)),
        (servo_speed_node, (bus, stop_event, args.speed, rng)),
        (speed_publisher_node, (bus, stop_event, args.speed)),
    ]:
        t = threading.Thread(target=target, args=targs, daemon=True)
        t.start(); threads.append(t)
    time.sleep(0.3 / args.speed)

    # T+6: database node + SCADA dashboard
    print("[SAAT] T+6: Starting database node + SCADA dashboard...")
    t = threading.Thread(target=data_collection_node,
                          args=(bus, state, packaging, conn, stop_event, args.speed), daemon=True)
    t.start(); threads.append(t)

    if not args.no_web:
        app = build_flask_app(bus, db_path)
        t = threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=args.port,
                                    debug=False, use_reloader=False, threaded=True),
            daemon=True)
        t.start(); threads.append(t)
        time.sleep(0.5)
        print(f"[SAAT] Dashboard: http://localhost:{args.port}")
        print(f"[SAAT] Database viewer: http://localhost:{args.port}/database")
        print(f"[SAAT] Package labels: http://localhost:{args.port}/labels")
        print(f"[SAAT] JSON API: http://localhost:{args.port}/api/status")
    else:
        print("[SAAT] --no-web: dashboard disabled (headless mode)")

    print("[SAAT] Full pipeline online. Press Ctrl+C to stop.")

    try:
        start = time.time()
        while True:
            time.sleep(0.5)
            if args.duration and (time.time() - start) >= args.duration:
                print(f"[SAAT] duration ({args.duration}s) reached, stopping.")
                break
    except KeyboardInterrupt:
        print("\n[SAAT] Ctrl+C received, stopping...")
    finally:
        stop_event.set()
        conn.close()
        print("[SAAT] Stopped. Database saved at:", db_path.resolve())


if __name__ == "__main__":
    main()