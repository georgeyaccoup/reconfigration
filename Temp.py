#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 SAAT  -  Solar-Powered Automated Agricultural Technology
 Pear Sorting & Packaging Line  -  Unified Vision + Control Production System
================================================================================

This is a single, self-contained implementation of the SAAT system described in
"SAAT_Vision_Control_Documentation.docx" and the accompanying architecture
overview, built directly on top of the hardware test scripts supplied for this
project:

    voltage_test.py     -> generalised into VoltageChannel (software PWM -> volts)
    distances.py         -> generalised into CameraSystem + the calibrated
                             6-zone grid geometry (draw_grid's tuned percentages)
    frames_test.py        -> merged into the same camera/grid geometry
    servo_gui.py           -> generalised into ServoController (PCA9685 / ServoKit)
    servos_testing.py      -> same PCA9685 channel map (A1..B3 -> ch 0..5)

Everything that was random-number simulation in the earlier `simulation.py`
proof-of-concept has been replaced here with real sensing and real actuation:

    * The 9-step classical CV pipeline (Section 6 of the documentation) is a
      real OpenCV pipeline (CLAHE -> bilateral filter -> Otsu -> morphology ->
      contour extraction -> infection colour-mask -> infection_ratio).
    * A lightweight shape/colour "is this even a pear" gate (aspect ratio,
      extent, solidity, circularity - Section 19.3 / Table 29) stands in for
      the TensorFlow classifier described in Section 7 (no trained model was
      supplied with this project), and is fused with the classical result by
      the same logical AND described in Section 7.1.
    * Volume/mass is computed from the real RealSense depth frame using the
      depth-integration method of Section 8.3, not a random number.
    * Conveyor speed is a real dual-channel PID (gains taken verbatim from
      Table 15) driving real software PWM -> RC low-pass filter -> PLC.
    * Six real MG995 servos are driven through a real PCA9685 (Section 12).

If the RealSense camera, Jetson.GPIO, or the PCA9685/ServoKit are not present
(e.g. running on a development laptop, or the CI sandbox that produced this
file), the system automatically drops into a clearly-labelled OFFLINE/DEV mode
for that piece of hardware only, exactly the same pattern already used in
servo_gui.py ("Running GUI in offline mode"). Nothing pretends to be real
hardware when it isn't - every simulated fallback prints/logs that it is
simulated. This lets the exact same file run end-to-end on the Jetson with the
full rig attached, or on a laptop for development/demo purposes.

NON-NEGOTIABLE SYSTEM CONSTRAINTS (Section 18) enforced throughout this file:
    1. 1-second action cycle           -> PIPELINE_BUDGET_S / per-zone timing log
    2. PWM -> LPF -> PLC is mandatory   -> VoltageChannel + wiring notes below
    3. Conv1_V + Conv2_V == 3.3 V       -> enforced in SpeedController.step()
    4. Max 6 pears in vision zone       -> SharedState.active_zone_count() -> CROWDED
    5. IoT data persists & syncs        -> SQLite is always written first,
                                            /api/status is derived from it
    6. classical_vision_initialization  -> INIT_EVENT barrier before any zone thread
       must run before any vision node     is allowed to process a frame
    7. DB writes are sequential A1->B3  -> DataCollectionNode polls ZONE_ORDER
                                            round-robin, in that exact order

HOW TO RUN
----------
    pip install opencv-python numpy flask
    # optional, only needed for the real hardware paths (auto-detected):
    pip install pyrealsense2 Jetson.GPIO adafruit-circuitpython-servokit

    python3 saat_system.py                  # auto-detect hardware, web on :8080
    python3 saat_system.py --force-sim       # force offline/dev mode everywhere
    python3 saat_system.py --no-web          # headless (node graph + DB only)
    python3 saat_system.py --duration 60     # auto-stop after 60 s

    Dashboard : http://localhost:8080
    Database  : http://localhost:8080/database
    Labels    : http://localhost:8080/labels
    JSON API  : http://localhost:8080/api/status
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

import numpy as np
import cv2

# ------------------------------------------------------------------------------
# OPTIONAL HARDWARE LIBRARIES - each is independently optional. The system
# degrades gracefully, one subsystem at a time, exactly like servo_gui.py's
# "hardware_active" pattern.
# ------------------------------------------------------------------------------
try:
    import pyrealsense2 as rs
    HAVE_REALSENSE = True
except ImportError:
    HAVE_REALSENSE = False

try:
    import Jetson.GPIO as GPIO
    HAVE_GPIO = True
except ImportError:
    HAVE_GPIO = False

try:
    from adafruit_servokit import ServoKit
    HAVE_SERVOKIT = True
except ImportError:
    HAVE_SERVOKIT = False

try:
    from flask import Flask, jsonify, render_template_string, Response
    HAVE_FLASK = True
except ImportError:
    HAVE_FLASK = False


########################################################################################
#
#   >>>>>>>>>>>>>>>>>>>>>>>>>>>>  CONTROL PANEL  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
#
#   Every value that governs the behaviour of this system lives in ONE place:
#   the CONFIG dict below (plus SHAPE_GATE and RUNTIME_DEFAULTS right after it).
#   Nothing that affects behaviour is hardcoded further down in the file - if
#   you want to change how the system acts, this is the only section you
#   should need to touch. Each block below is grouped by subsystem and cites
#   the documentation section/table it comes from, and flags whether it is a
#   MEASURED/DOCUMENTED value (safe to trust) or a RIG-SPECIFIC value you
#   must calibrate for your own physical setup (marked "<-- CALIBRATE").
#
########################################################################################

ZONE_ORDER = ["A1", "A2", "A3", "B1", "B2", "B3"]          # Sections 4.2 / 14.1
MOTOR_CHANNEL = {"A1": 0, "A2": 1, "A3": 2, "B1": 3, "B2": 4, "B3": 5}  # PCA9685 channels, servos_testing.py

CONFIG = {

    # ==========================================================================
    # 1) CAMERA  -  Section 3 / Table 1
    # ==========================================================================
    "camera": {
        "color_width": 1280, "color_height": 800, "color_fps": 30,     # <-- CALIBRATE: raise toward 90 Hz only if your D455/USB link can sustain it
        "depth_width": 1280, "depth_height": 720, "depth_fps": 30,     # <-- CALIBRATE: RealSense depth mode; doc target is 60 Hz
        "working_distance_mm": 380,        # belt-to-camera height, Section 3.1/8.2  <-- CALIBRATE per gantry leadscrew position
    },

    # ==========================================================================
    # 2) FRAME LAYOUT  -  Table 3: raw 1280 px-wide colour frame column split
    # ==========================================================================
    "frame_columns": {"conv1": (0, 256), "vision": (257, 1024), "conv2": (1025, 1280)},

    # ==========================================================================
    # 3) ZONE GRID  -  the calibrated 6-zone (A1..B3) grid, carried over from
    #    distances.py / frames_test.py draw_grid(). THESE ARE THE FIRST NUMBERS
    #    TO RE-TUNE if the red zone lines don't line up with your physical rig.
    # ==========================================================================
    "zone_grid": {
        "crop_x1": 180, "crop_x2": 1030, "crop_y1": 0, "crop_y2": 800,   # <-- CALIBRATE: outer belt crop
        "top_margin_pct": 0.08,     # C1 boundary / pear entry line, t1        <-- CALIBRATE
        "mid_y_pct": 0.52,          # Row A / Row B divider                    <-- CALIBRATE
        "bottom_margin_pct": 0.95,  # C2 boundary / pear exit line, t2         <-- CALIBRATE
        "vert_left_pct": 0.30,      # lane 1 / lane 2 divider                  <-- CALIBRATE
        "vert_right_pct": 0.63,     # lane 2 / lane 3 divider                  <-- CALIBRATE
    },
    "zone_row_spacing_m": 0.15,     # physical A-row -> B-row distance         <-- CALIBRATE (measure on the rig)

    # ==========================================================================
    # 4) VISION / INFECTION DETECTION  -  Section 6 / Table 30 (production row)
    # ==========================================================================
    "vision": {
        "min_pear_area_px": 800,                     # ignore contours smaller than this (noise) <-- CALIBRATE
        "infection_ratio_threshold": 0.05,            # Section 9: REJECT if infection_ratio > this (5%)
        "infection_hsv_lo": (0, 0, 0),                # dark/low-value blemish band, lower HSV bound
        "infection_hsv_hi": (180, 150, 80),           # dark/low-value blemish band, upper HSV bound  <-- CALIBRATE to your lighting
        "clahe_clip": 2.0, "clahe_tile": (8, 8),       # CLAHE contrast enhancement (step 3 of 9)
        "bilateral_d": 9, "bilateral_sigma": 75,       # bilateral filter smoothing (step 4 of 9)
        "morph_kernel_size": 5,                        # morphological open/close kernel, px (step 6 of 9)
    },
    "big_small_threshold_px2": 15000,               # Section 8.6: BIG/SMALL category cutoff

    # ==========================================================================
    # 5) SHAPE / SECOND-OPINION GATE  -  Section 19.3 / Table 29. Stands in for
    #    the (untrained) TensorFlow classifier of Section 7, AND-fused with the
    #    classical infection check per Section 7.1.
    # ==========================================================================
    "shape_gate": {
        "aspect_ratio": (1.1, 2.5),
        "extent": (0.5, 0.9),
        "solidity_min": 0.85,
        "circularity": (0.4, 0.9),
        "min_checks_passed": 3,   # out of 4 checks must pass to call it "a pear"
    },

    # ==========================================================================
    # 6) MASS / VOLUME MODEL  -  Section 8.2-8.4
    # ==========================================================================
    "mass_model": {"density_g_cm3": 0.960, "intercept_g": -0.02},   # Mass_g = density*Volume_cm3 + intercept
    "px_to_mm2": 0.070,   # 1 silhouette pixel -> mm^2 of real belt area at 380 mm  <-- CALIBRATE for your D455's focal length

    # ==========================================================================
    # 7) SPEED CONTROL  -  Section 10 / Table 15 (PID gains) + Section 10.4
    # ==========================================================================
    "pid_conv1": {"kp": 0.16, "ki": 11.76, "kd": 0.020},   # Conv1 (175 cm loading belt)
    "pid_conv2": {"kp": 0.14, "ki": 15.45, "kd": 0.015},   # Conv2 (115 cm packing belt)
    "pid_servo": {"kp": 0.07, "ki": 11.12, "kd": 0.010},   # servo return-delay loop (reserved for future use)
    "max_ref_speed_ms": 0.5,          # Section 10.4 cap on the computed reference speed
    "speed_control": {
        "loop_dt_s": 0.1,             # PID/PWM update period, 10 Hz per Section 10.3
        "pid_trim_gain": 0.01,        # how strongly the PID correction nudges the target voltage  <-- TUNE for your motors
    },
    "max_pears_in_vision_zone": 6,    # Section 18 hard capacity limit -> CROWDED/overflow mode

    # ==========================================================================
    # 8) CONVEYOR VOLTAGE / PWM  -  Section 11 / Table 18: PWM -> LPF -> PLC
    # ==========================================================================
    "speed_publisher": {
        "gpio_pin_conv1": 11,     # physical pin 11 / GPIO17 (Table 2/26)        <-- CALIBRATE to your wiring
        "gpio_pin_conv2": 13,     # physical pin 13 / GPIO27                     <-- CALIBRATE to your wiring
        "pwm_frequency_hz": 500,
        "min_voltage": 0.1,       # Section 18.1: never true 0 V (PLC would read it as a fault)
        "max_voltage": 3.3,
        "lpf_r_ohm": 10_000, "lpf_c_f": 10e-6,   # external RC low-pass filter (real hardware, not code) -> fc ~= 1.59 Hz
    },

    # ==========================================================================
    # 9) SERVO ACTUATION  -  Section 12 / Table 19
    # ==========================================================================
    "servo": {
        "angle_rejected": 0.0, 
        "angle_home": 80.0, 
        "angle_accepted": 175.0,
        "return_delay_s": 0.4,
        "pulse_min_us": 500,           
        "pulse_max_us": 2500,          
        "pwm_freq_hz": 50,
        "occupancy_cooldown_s": 3.0,

    },

    # ==========================================================================
    # 10) LIVE CAMERA VIEW  -  "what the camera sees / what it does": streams
    #     the annotated colour feed to the dashboard at /camera (/video_feed)
    # ==========================================================================
    "camera_stream": {
        "enabled": True,
        "target_fps": 10,             # how often a new overlay frame is composited/streamed
        "jpeg_quality": 75,           # 0-100, higher = sharper but more bandwidth
        "show_zone_grid": True,
        "show_status_banner": True,
    },

    # ==========================================================================
    # 11) PACKAGING  -  Section 15.3 / Figure 15.2
    # ==========================================================================
    "packaging": {"big_per_package": 12, "small_per_package": 12, "company_name": "SAAT"},

    # ==========================================================================
    # 12) TIMING / POLLING  -  Section 13 loop cadences and the 1 s cycle budget
    # ==========================================================================
    "iot_publish_period_s": 10.0,        # Section 14.3: 0.1 Hz cloud/dashboard publish
    "action_cycle_budget_s": 1.0,        # Section 13: hard 1-second action-cycle deadline
    "timing": {
        "zone_poll_interval_s": 0.02,    # how often each zone thread re-checks its crop for a pear
        "idle_sleep_s": 0.05,            # sleep when a loop finds no new work (camera/db threads)
    },

    # ==========================================================================
    # 13) DEV / OFFLINE SIMULATION MODE  -  only used when no RealSense camera
    #     is detected (or --force-sim is passed); controls the synthetic pears
    #     used to exercise the pipeline without physical hardware attached.
    # ==========================================================================
    "dev_mode": {
        "spawn_probability_per_frame": 0.02,
        "max_concurrent_sim_pears": 5,
        "pear_radius_px_range": (45, 70),
        "infection_probability": 0.30,
        "belt_fall_speed_px_per_frame": 6.0,
        "random_seed": 42,
    },

    # ==========================================================================
    # 14) DASHBOARD  -  Section 15 SCADA web publisher (Flask, port 8080)
    # ==========================================================================
    "dashboard": {
        "colors": {"bg": "#0d1117", "surface": "#161b22", "border": "#30363d",
                   "green": "#00ff88", "amber": "#f59e0b", "red": "#ef4444", "blue": "#3b82f6"},
        "auto_refresh_s": 10,            # status-page <meta refresh>, matches iot_publish_period_s
        "database_rows_shown": 200,
    },
}

# Convenience alias so `SHAPE_GATE` keeps working anywhere it's referenced
# directly (it now lives inside CONFIG, section 5 above, as the single
# source of truth - edit it there).
SHAPE_GATE = CONFIG["shape_gate"]

########################################################################################
# RUNTIME DEFAULTS  -  the default value of every command-line flag (main()'s
# argparse). Change these if you want a different out-of-the-box behaviour
# without having to type flags every time; every one of these can still be
# overridden on the command line (--port, --db-path, etc).
########################################################################################
RUNTIME_DEFAULTS = {
    "port": 8080,
    "no_web": False,
    "duration": 0.0,          # 0 = run until Ctrl+C
    "db_path": "./saat_data/saat_records.db",
    "force_sim": False,
}


# ==============================================================================
# DATA MODELS  -  Section 14.3 (13-field IoT schema) / Table 21 (DB schema)
# ==============================================================================
@dataclass
class PearData:
    pear_id: str
    zone: str
    status: str            # ACCEPTED | REJECTED
    category: str          # BIG | SMALL
    infection_area_px: float
    infection_loc: tuple    # (x, y)
    infection_rgb: tuple    # (R, G, B)
    infection_ratio: float
    surface_area_px: float
    volume_cm3: float
    mass_g: float
    belt_speed_m_s: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class PackageLabel:
    package_id: str
    upper_layer: list       # 12x {"position","pear_id","mass_g"}  (BIG)
    lower_layer: list       # 12x {"position","pear_id","mass_g"}  (SMALL)
    upper_weight_g: float
    lower_weight_g: float
    total_weight_g: float
    start_time: float
    end_time: float
    duration_s: float


# ==============================================================================
# SHARED STATE  -  cross-thread counters / timing, Section 10.4 & 18
# ==============================================================================
class SharedState:
    def __init__(self):
        self._lock = threading.Lock()
        self._active = {z: False for z in ZONE_ORDER}
        self._last_action = {z: "IDLE" for z in ZONE_ORDER}
        self._pear_counter = {z: 0 for z in ZONE_ORDER}
        self._row_a_entry = {}      # lane -> t1 (Section 4.2 timing marks)
        self._recent_speeds = deque(maxlen=20)
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

    def motor_status_array(self):
        with self._lock:
            return [{"zone": z, "active": self._active[z], "last_action": self._last_action[z]}
                    for z in ZONE_ORDER]

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

    # --- Section 10.4 reference-speed algorithm: t1 (row A) / t2 (row B) ---
    def mark_row_a(self, lane):
        with self._lock:
            self._row_a_entry[lane] = time.time()

    def mark_row_b(self, lane, row_spacing_m):
        with self._lock:
            t1 = self._row_a_entry.pop(lane, None)
            if t1 is None:
                return None
            dt = max(time.time() - t1, 1e-3)
            speed = row_spacing_m / dt
            self._recent_speeds.append(speed)
            return speed

    def average_pear_speed(self):
        with self._lock:
            if not self._recent_speeds:
                return None
            return sum(self._recent_speeds) / len(self._recent_speeds)


# ==============================================================================
# PACKAGING MANAGER  -  Section 15.3: 12 BIG (upper) + 12 SMALL (lower) -> label
# ==============================================================================
class PackagingManager:
    def __init__(self, big_count, small_count):
        self.big_count = big_count
        self.small_count = small_count
        self._lock = threading.Lock()
        self._big_q = deque()
        self._small_q = deque()
        self.package_counter = 0

    def add_pear(self, pear: PearData):
        if pear.status != "ACCEPTED":
            return None   # rejected pears are ejected, never packaged
        now = time.time()
        with self._lock:
            (self._big_q if pear.category == "BIG" else self._small_q).append((pear, now))
            if len(self._big_q) < self.big_count or len(self._small_q) < self.small_count:
                return None
            big_items = [self._big_q.popleft() for _ in range(self.big_count)]
            small_items = [self._small_q.popleft() for _ in range(self.small_count)]
            self.package_counter += 1
            package_id = f"PA{self.package_counter:05d}"
            upper_layer = [{"position": f"P{i+1}", "pear_id": p.pear_id, "mass_g": p.mass_g}
                           for i, (p, _) in enumerate(big_items)]
            lower_layer = [{"position": f"P{i+1}", "pear_id": p.pear_id, "mass_g": p.mass_g}
                           for i, (p, _) in enumerate(small_items)]
            upper_weight_g = round(sum(p.mass_g for p, _ in big_items), 2)
            lower_weight_g = round(sum(p.mass_g for p, _ in small_items), 2)
            start_time = min(big_items[0][1], small_items[0][1])
            return PackageLabel(
                package_id=package_id, upper_layer=upper_layer, lower_layer=lower_layer,
                upper_weight_g=upper_weight_g, lower_weight_g=lower_weight_g,
                total_weight_g=round(upper_weight_g + lower_weight_g, 2),
                start_time=start_time, end_time=now, duration_s=round(now - start_time, 2),
            )


# ==============================================================================
# DATABASE  -  Table 21 (pear_records, 13 fields) + packages (Section 15.3)
# ==============================================================================
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS pear_records (
    pear_id             TEXT PRIMARY KEY,
    zone                TEXT,
    timestamp           REAL,
    status              TEXT,
    category            TEXT,
    infection_area_px   REAL,
    infection_loc       TEXT,
    infection_rgb       TEXT,
    infection_ratio     REAL,
    surface_area_px     REAL,
    volume_cm3          REAL,
    mass_g              REAL,
    belt_speed_m_s      REAL
);

CREATE TABLE IF NOT EXISTS packages (
    package_id       TEXT PRIMARY KEY,
    start_timestamp  REAL,
    end_timestamp    REAL,
    duration_s       REAL,
    upper_layer      TEXT,
    lower_layer      TEXT,
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
# PID  -  textbook discrete controller, gains from Table 15
# ==============================================================================
class PID:
    def __init__(self, kp, ki, kd, out_min=None, out_max=None):
        self.kp, self.ki, self.kd = kp, ki, kd
        self.out_min, self.out_max = out_min, out_max
        self.integral = 0.0
        self.prev_error = 0.0

    def step(self, setpoint, measurement, dt):
        error = setpoint - measurement
        self.integral += error * dt
        derivative = (error - self.prev_error) / dt if dt > 0 else 0.0
        self.prev_error = error
        out = self.kp * error + self.ki * self.integral + self.kd * derivative
        if self.out_min is not None:
            out = max(out, self.out_min)
        if self.out_max is not None:
            out = min(out, self.out_max)
        return out


# ==============================================================================
# VOLTAGE CHANNEL  -  generalisation of voltage_test.py's software-PWM
# generator into a reusable class, one instance per conveyor (Conv1/Conv2).
# Jetson Nano -> PWM (500 Hz) -> external RC Low-Pass Filter -> 0-3.3 V -> PLC
# (Section 11 / Table 18). This class only produces the PWM; the RC filter is
# real external hardware (R = 10 kOhm, C = 10 uF -> fc ~= 1.59 Hz, Table 18)
# that must be wired between the GPIO pin and the PLC analog input.
# ==============================================================================
_gpio_mode_lock = threading.Lock()
_gpio_mode_ready = False

def _ensure_gpio_mode():
    """Jetson.GPIO.setmode()/setwarnings() are process-global and may only be
    called ONCE - calling it again (even with the same mode) from a second
    VoltageChannel raises 'A different mode has already been set!'. This
    guard makes sure it only ever runs a single time, no matter how many
    VoltageChannel instances are created."""
    global _gpio_mode_ready
    with _gpio_mode_lock:
        if not _gpio_mode_ready:
            GPIO.setwarnings(False)
            
            # --- FIX: Clear Adafruit Blinka's conflicting mode lock ---
            current_mode = GPIO.getmode()
            if current_mode is not None and current_mode != GPIO.BOARD:
                GPIO.cleanup()
            # ----------------------------------------------------------
            
            GPIO.setmode(GPIO.BOARD)
            _gpio_mode_ready = True


class VoltageChannel:
    def __init__(self, pin: int, freq_hz: float, max_voltage: float, label: str):
        self.pin = pin
        self.period = 1.0 / freq_hz
        self.max_voltage = max_voltage
        self.label = label
        self.duty_cycle = 0.0
        self._lock = threading.Lock()
        self._running = True
        if HAVE_GPIO:
            _ensure_gpio_mode()
            GPIO.setup(self.pin, GPIO.OUT)
        else:
            print(f"[VoltageChannel:{label}] Jetson.GPIO not available - "
                  f"running in simulated/offline mode (pin {pin}).")
        self._thread = threading.Thread(target=self._pwm_loop, daemon=True)
        self._thread.start()

    def set_voltage(self, voltage: float):
        voltage = min(max(voltage, 0.0), self.max_voltage)
        with self._lock:
            self.duty_cycle = voltage / self.max_voltage

    def get_voltage(self):
        with self._lock:
            return self.duty_cycle * self.max_voltage

    def _pwm_loop(self):
        while self._running:
            with self._lock:
                d = self.duty_cycle
            if not HAVE_GPIO:
                time.sleep(self.period)
                continue
            if d <= 0.0:
                GPIO.output(self.pin, GPIO.LOW)
                time.sleep(0.01)
            elif d >= 1.0:
                GPIO.output(self.pin, GPIO.HIGH)
                time.sleep(0.01)
            else:
                t_on = self.period * d
                t_off = self.period * (1.0 - d)
                GPIO.output(self.pin, GPIO.HIGH)
                time.sleep(t_on)
                GPIO.output(self.pin, GPIO.LOW)
                time.sleep(t_off)

    def stop(self):
        self._running = False
        self._thread.join(timeout=1.0)
        if HAVE_GPIO:
            GPIO.output(self.pin, GPIO.LOW)


# ==============================================================================
# SERVO CONTROLLER  -  generalisation of servo_gui.py / servos_testing.py.
# Six MG995 servos (A1..B3) on one PCA9685, channels 0-5 (Section 12/Table 19).
# ==============================================================================

class ServoController:
    def __init__(self, cfg):
        self.cfg = cfg
        self.hardware_active = HAVE_SERVOKIT
        self._lock = threading.Lock()
        if self.hardware_active:
            try:
                self.kit = ServoKit(channels=16)
                for ch in MOTOR_CHANNEL.values():
                    self.kit.servo[ch].set_pulse_width_range(cfg["pulse_min_us"], cfg["pulse_max_us"])
                    # Initialize aggressively to the HOME position
                    self.kit.servo[ch].angle = cfg["angle_home"]
                print("[ServoController] PCA9685 initialised, all servos snapped to HOME (80 deg).")
            except Exception as e:
                print(f"[ServoController] Hardware error: {e}. Falling back to offline mode.")
                self.hardware_active = False
        else:
            print("[ServoController] adafruit_servokit not available - offline mode.")

    def set_angle(self, zone: str, angle: float):
        channel = MOTOR_CHANNEL[zone]
        with self._lock:
            if self.hardware_active:
                self.kit.servo[channel].angle = angle
            else:
                print(f"[servo:sim] {zone} (ch {channel}) -> {angle:.0f} deg")

    def dispatch(self, zone: str, accepted: bool):
        """Triggers the cascaded series sequence based on the A-row detection."""
        
        # Since A and B are linked in series, we only trigger the physical sequence 
        # when the A-zone detects the pear. We ignore independent B-zone triggers.
        if zone.startswith("B"):
            return
            
        # Extract the lane number ('1', '2', or '3') to pair A and B together
        lane_num = zone[1] 
        zone_a = f"A{lane_num}"
        zone_b = f"B{lane_num}"
        
        # Launch the sequence in a background thread to avoid blocking the vision pipeline
        threading.Thread(
            target=self._series_move_sequence, 
            args=(zone_a, zone_b, accepted), 
            daemon=True
        ).start()

    def _series_move_sequence(self, zone_a: str, zone_b: str, accepted: bool):
        target_angle = self.cfg["angle_accepted"] if accepted else self.cfg["angle_rejected"]
        
        # Step 1: A moves to target. B is explicitly held at Home.
        self.set_angle(zone_a, target_angle)
        self.set_angle(zone_b, self.cfg["angle_home"])
        
        # Step 2: Delay 0.5s while A is operating
        time.sleep(0.5)
        
        # Step 3: A returns Home. B moves to target.
        self.set_angle(zone_a, self.cfg["angle_home"])
        self.set_angle(zone_b, target_angle)
        
        # Step 4: Wait for the pear to clear the B gate
        time.sleep(self.cfg["return_delay_s"])
        
        # Step 5: B returns Home, resetting the lane
        self.set_angle(zone_b, self.cfg["angle_home"])


# ==============================================================================
# ZONE GEOMETRY  -  the calibrated 6-zone grid from distances.py / frames_test.py
# (draw_grid's tuned percentages), Section 4 / Table 3 / Table 4.
# ==============================================================================
def zone_rectangles(crop_w: int, crop_h: int, grid_cfg: dict):
    """Return {zone_name: (x1, y1, x2, y2)} inside the cropped colour/depth frame."""
    top_y = int(crop_h * grid_cfg["top_margin_pct"])
    mid_y = int(crop_h * grid_cfg["mid_y_pct"])
    bot_y = int(crop_h * grid_cfg["bottom_margin_pct"])
    left_x = int(crop_w * grid_cfg["vert_left_pct"])
    right_x = int(crop_w * grid_cfg["vert_right_pct"])
    lanes = [(0, left_x), (left_x, right_x), (right_x, crop_w)]
    rects = {}
    for lane_idx, (lx1, lx2) in enumerate(lanes, start=1):
        rects[f"A{lane_idx}"] = (lx1, top_y, lx2, mid_y)     # Row A: entry line (t1)
        rects[f"B{lane_idx}"] = (lx1, mid_y, lx2, bot_y)     # Row B: exit line (t2)
    return rects


ZONE_STATUS_COLOR_BGR = {
    "IDLE": (140, 140, 140),
    "ACCEPTED": (0, 220, 0),
    "REJECTED": (0, 0, 230),
}


def draw_zone_overlay(img, rects, motors_status=None):
    """Debug/HMI overlay - zone grid lines + labels, colour-coded by each
    zone's last decision (grey=idle, green=accepted, red=rejected) so the
    live feed shows both what the camera sees AND what the system decided."""
    status_by_zone = {m["zone"]: m for m in (motors_status or [])}
    for zone, (x1, y1, x2, y2) in rects.items():
        m = status_by_zone.get(zone)
        last_action = m["last_action"] if m else "IDLE"
        active = m["active"] if m else False
        color = ZONE_STATUS_COLOR_BGR.get(last_action, ZONE_STATUS_COLOR_BGR["IDLE"])
        thickness = 4 if active else 2
        cv2.rectangle(img, (x1, y1), (x2, y2), color, thickness)
        label = f"{zone}: {last_action}" if last_action != "IDLE" else zone
        cv2.putText(img, label, (x1 + 6, y1 + 24), cv2.FONT_HERSHEY_SIMPLEX,
                    0.6, (0, 255, 255), 2, cv2.LINE_AA)
    return img


class VideoStreamBuffer:
    """Thread-safe holder for the latest JPEG-encoded annotated frame,
    consumed by the Flask /video_feed MJPEG endpoint."""

    def __init__(self):
        self._lock = threading.Lock()
        self._jpeg_bytes = None

    def set_frame(self, jpeg_bytes: bytes):
        with self._lock:
            self._jpeg_bytes = jpeg_bytes

    def get_frame(self):
        with self._lock:
            return self._jpeg_bytes


def video_stream_node(framebuf: "FrameBuffer", state: "SharedState", rects: dict,
                       stream_buf: VideoStreamBuffer, stop_event: threading.Event, cfg: dict):
    """'Let me see what the camera sees, and what it does': continuously
    composites the live cropped colour frame with the zone grid and each
    zone's live accept/reject status, JPEG-encodes it, and publishes it to
    the dashboard's /video_feed. This is purely a viewer - it does not feed
    back into any decision."""
    sc = cfg["camera_stream"]
    if not sc["enabled"]:
        return
    period = 1.0 / max(sc["target_fps"], 1)
    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), sc["jpeg_quality"]]
    while not stop_event.is_set():
        color, _depth, _fid = framebuf.get()
        if color is None:
            time.sleep(period)
            continue
        frame = color.copy()
        if sc["show_zone_grid"]:
            draw_zone_overlay(frame, rects, state.motor_status_array())
        if sc["show_status_banner"]:
            banner = (f"ACCEPTED {state.batch_accepted}   REJECTED {state.batch_rejected}   "
                      f"PACKAGES {state.completed_packages}   IN VISION ZONE {state.active_zone_count()}")
            cv2.rectangle(frame, (0, 0), (frame.shape[1], 34), (20, 20, 20), -1)
            cv2.putText(frame, banner, (10, 24), cv2.FONT_HERSHEY_SIMPLEX,
                        0.65, (0, 255, 136), 2, cv2.LINE_AA)
        ok, jpeg = cv2.imencode(".jpg", frame, encode_params)
        if ok:
            stream_buf.set_frame(jpeg.tobytes())
        time.sleep(period)


# ==============================================================================
# CAMERA SYSTEM  -  generalisation of distances.py / frames_test.py.
# Real Intel RealSense D455 when available (Section 3.1); otherwise a clearly
# labelled synthetic frame generator so the whole pipeline can be developed
# and demonstrated without the physical rig attached.
# ==============================================================================
class CameraSystem:
    def __init__(self, full_cfg):
        self.cfg = full_cfg["camera"]
        self.dev_cfg = full_cfg["dev_mode"]
        cfg = self.cfg
        self.hardware_active = HAVE_REALSENSE
        self.depth_scale = 0.001   # metres per depth unit, RealSense default
        self._sim_rng = random.Random(self.dev_cfg["random_seed"])
        self._sim_pears = []

        if self.hardware_active:
            try:
                self.pipeline = rs.pipeline()
                rs_cfg = rs.config()
                rs_cfg.enable_stream(rs.stream.color, cfg["color_width"], cfg["color_height"],
                                      rs.format.bgr8, cfg["color_fps"])
                rs_cfg.enable_stream(rs.stream.depth, cfg["depth_width"], cfg["depth_height"],
                                      rs.format.z16, cfg["depth_fps"])
                profile = self.pipeline.start(rs_cfg)
                self.depth_scale = profile.get_device().first_depth_sensor().get_depth_scale()
                self.align = rs.align(rs.stream.color)
                self.spatial = rs.spatial_filter()
                self.temporal = rs.temporal_filter()
                self.hole_filling = rs.hole_filling_filter()
                print("[CameraSystem] RealSense D455 online "
                      f"({cfg['color_width']}x{cfg['color_height']} colour, "
                      f"{cfg['depth_width']}x{cfg['depth_height']} depth).")
            except Exception as e:
                print(f"[CameraSystem] Could not start RealSense ({e}); "
                      "falling back to simulated frames.")
                self.hardware_active = False
        if not self.hardware_active:
            print("[CameraSystem] pyrealsense2 not available - generating "
                  "SIMULATED colour+depth frames (offline/dev mode).")

    def get_frames(self):
        """Returns (color_bgr uint8 HxWx3, depth_m float32 HxW) or (None, None)."""
        if self.hardware_active:
            frames = self.pipeline.wait_for_frames()
            aligned = self.align.process(frames)
            depth_frame = aligned.get_depth_frame()
            color_frame = aligned.get_color_frame()
            if not depth_frame or not color_frame:
                return None, None
            depth_frame = self.spatial.process(depth_frame)
            depth_frame = self.temporal.process(depth_frame)
            depth_frame = self.hole_filling.process(depth_frame)
            color = np.asanyarray(color_frame.get_data())
            depth_raw = np.asanyarray(depth_frame.get_data())
            depth_m = depth_raw.astype(np.float32) * self.depth_scale
            return color, depth_m
        return self._synthetic_frame()

    def _synthetic_frame(self):
        """Offline/dev-mode stand-in: a belt-coloured background with a few
        randomly-placed, randomly-blemished circular 'pears' drifting through
        frame, so the full CV/decision/DB/dashboard pipeline is exercisable
        without the physical rig."""
        dev = self.dev_cfg
        w, h = self.cfg["color_width"], self.cfg["color_height"]
        color = np.full((h, w, 3), (60, 90, 40), dtype=np.uint8)   # dull belt green

        r_lo, r_hi = dev["pear_radius_px_range"]
        if (self._sim_rng.random() < dev["spawn_probability_per_frame"]
                and len(self._sim_pears) < dev["max_concurrent_sim_pears"]):
            self._sim_pears.append({
                "x": float(self._sim_rng.uniform(0.15 * w, 0.85 * w)),
                "y": -30.0,
                "r": float(self._sim_rng.uniform(r_lo, r_hi)),
                "infected": bool(self._sim_rng.random() < dev["infection_probability"]),
                "color": (int(self._sim_rng.uniform(30, 70)),
                          int(self._sim_rng.uniform(140, 200)),
                          int(self._sim_rng.uniform(150, 210))),
            })

        # depth aligned 1:1 to colour resolution, exactly what rs.align() gives
        # us on the real camera - keeps every downstream consumer identical
        # whether it's fed by real or simulated frames.
        depth_m = np.full((h, w), self.cfg["working_distance_mm"] / 1000.0, dtype=np.float32)

        survivors = []
        for p in self._sim_pears:
            p["y"] += dev["belt_fall_speed_px_per_frame"]
            if p["y"] - p["r"] > h:
                continue
            survivors.append(p)
            cv2.circle(color, (int(p["x"]), int(p["y"])), int(p["r"]), p["color"], -1)
            if p["infected"]:
                cv2.circle(color, (int(p["x"] - p["r"] * 0.25), int(p["y"] + p["r"] * 0.2)),
                           max(3, int(p["r"] * 0.28)), (25, 25, 25), -1)
            cv2.circle(depth_m, (int(p["x"]), int(p["y"])), int(p["r"]),
                       float((self.cfg["working_distance_mm"] - 55) / 1000.0), -1)
        self._sim_pears = survivors
        return color, depth_m

    def stop(self):
        if self.hardware_active:
            self.pipeline.stop()


# ==============================================================================
# 9-STEP CLASSICAL CV PIPELINE  -  Section 6.1, real OpenCV implementation.
#   1) ROI crop (done by the caller via zone_rectangles)
#   2) BGR -> LAB colour-space conversion
#   3) CLAHE contrast enhancement on the L channel
#   4) Bilateral filtering (edge-preserving smoothing)
#   5) Otsu thresholding (pear-vs-background)
#   6) Morphological cleanup (open + close)
#   7) Contour extraction & pear geometry (surface_area_px, centroid)
#   8) Infection colour-mask detection inside the pear silhouette
#   9) Feature aggregation -> infection_ratio
# ==============================================================================
def run_classical_vision(zone_bgr, vcfg):
    lab = cv2.cvtColor(zone_bgr, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=vcfg["clahe_clip"], tileGridSize=vcfg["clahe_tile"])
    l_eq = clahe.apply(l)
    enhanced = cv2.cvtColor(cv2.merge([l_eq, a, b]), cv2.COLOR_LAB2BGR)

    smooth = cv2.bilateralFilter(enhanced, vcfg["bilateral_d"],
                                  vcfg["bilateral_sigma"], vcfg["bilateral_sigma"])
    hsv = cv2.cvtColor(smooth, cv2.COLOR_BGR2HSV)

    sat = hsv[:, :, 1]
    _, mask = cv2.threshold(sat, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    k = vcfg["morph_kernel_size"]
    kernel = np.ones((k, k), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return None
    pear_contour = max(contours, key=cv2.contourArea)
    surface_area_px = float(cv2.contourArea(pear_contour))
    if surface_area_px < vcfg["min_pear_area_px"]:
        return None

    pear_mask = np.zeros(mask.shape, np.uint8)
    cv2.drawContours(pear_mask, [pear_contour], -1, 255, -1)
    m = cv2.moments(pear_contour)
    cx = m["m10"] / m["m00"] if m["m00"] else 0.0
    cy = m["m01"] / m["m00"] if m["m00"] else 0.0

    infect_raw = cv2.inRange(hsv, vcfg["infection_hsv_lo"], vcfg["infection_hsv_hi"])
    infect_mask = cv2.bitwise_and(infect_raw, pear_mask)
    infect_mask = cv2.morphologyEx(infect_mask, cv2.MORPH_OPEN, kernel)
    icontours, _ = cv2.findContours(infect_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    infection_area_px = float(sum(cv2.contourArea(c) for c in icontours))

    if icontours:
        ic = max(icontours, key=cv2.contourArea)
        im = cv2.moments(ic)
        ix = im["m10"] / im["m00"] if im["m00"] else cx
        iy = im["m01"] / im["m00"] if im["m00"] else cy
        mean_bgr = cv2.mean(zone_bgr, mask=infect_mask)
        infection_rgb = (int(mean_bgr[2]), int(mean_bgr[1]), int(mean_bgr[0]))
    else:
        ix, iy = cx, cy
        infection_rgb = (0, 0, 0)

    infection_ratio = infection_area_px / surface_area_px if surface_area_px else 0.0

    return {
        "pear_contour": pear_contour, "pear_mask": pear_mask,
        "surface_area_px": surface_area_px, "centroid": (cx, cy),
        "infection_area_px": infection_area_px, "infection_loc": (ix, iy),
        "infection_rgb": infection_rgb, "infection_ratio": infection_ratio,
    }


def shape_gate_score(pear_contour, pear_mask, hsv_zone, gate_cfg):
    """Section 19.3 / Table 29 - the 4-check shape/colour gate, standing in
    for the TensorFlow classifier of Section 7 (no trained model supplied).
    Returns True (GOOD/valid pear) if >= min_checks_passed of 4 checks pass."""
    x, y, w, h = cv2.boundingRect(pear_contour)
    area = cv2.contourArea(pear_contour)
    perimeter = cv2.arcLength(pear_contour, True)
    hull = cv2.convexHull(pear_contour)
    hull_area = cv2.contourArea(hull)

    checks = 0
    if h > 0 and w > 0:
        ar = h / w
        if gate_cfg["aspect_ratio"][0] < ar < gate_cfg["aspect_ratio"][1]:
            checks += 1
    if w * h > 0:
        extent = area / (w * h)
        if gate_cfg["extent"][0] < extent < gate_cfg["extent"][1]:
            checks += 1
    if hull_area > 0:
        solidity = area / hull_area
        if solidity > gate_cfg["solidity_min"]:
            checks += 1
    if perimeter > 0:
        circularity = 4 * np.pi * area / (perimeter ** 2)
        if gate_cfg["circularity"][0] < circularity < gate_cfg["circularity"][1]:
            checks += 1

    return checks >= gate_cfg["min_checks_passed"]


def estimate_volume_mass(depth_m, pear_mask, working_distance_mm, px_to_mm2, mass_cfg):
    """Section 8.3: Volume = sum(pixel_area_mm2 * height_above_belt_mm), then
    Section 8.4: Mass_g = rho * Volume_cm3 + b."""
    if depth_m.shape != pear_mask.shape:
        depth_m = cv2.resize(depth_m, (pear_mask.shape[1], pear_mask.shape[0]))
    depth_mm = depth_m * 1000.0
    height_mm = np.clip(working_distance_mm - depth_mm, 0.0, None)
    pear_pixels = pear_mask > 0
    volume_mm3 = float(np.sum(height_mm[pear_pixels]) * px_to_mm2)
    volume_cm3 = volume_mm3 / 1000.0
    mass_g = mass_cfg["density_g_cm3"] * volume_cm3 + mass_cfg["intercept_g"]
    return round(volume_cm3, 2), round(max(mass_g, 0.0), 2)


# ==============================================================================
# FRAME BUFFER  -  Section 2.1 Layer 1 (frame_divider / frame_speed_divider /
# volume_divider): one captured frame, fanned out to every zone consumer.
# ==============================================================================
class FrameBuffer:
    def __init__(self):
        self._lock = threading.Lock()
        self.color = None
        self.depth = None
        self.frame_id = 0

    def update(self, color, depth):
        with self._lock:
            self.color, self.depth, self.frame_id = color, depth, self.frame_id + 1

    def get(self):
        with self._lock:
            return self.color, self.depth, self.frame_id


def frame_capture_node(camera: CameraSystem, framebuf: FrameBuffer,
                        stop_event: threading.Event, cfg: dict):
    """Layer 0->1: opens the RGB-D stream and republishes the belt-cropped
    colour+depth pair for every downstream consumer (Section 2.1)."""
    grid = cfg["zone_grid"]
    period = 1.0 / cfg["camera"]["color_fps"]
    print("[frame_capture_node] streaming...")
    while not stop_event.is_set():
        color, depth = camera.get_frames()
        if color is None:
            time.sleep(period)
            continue
        x1, x2 = grid["crop_x1"], grid["crop_x2"]
        y1, y2 = grid["crop_y1"], grid["crop_y2"]
        framebuf.update(color[y1:y2, x1:x2].copy(), depth[y1:y2, x1:x2].copy())
        time.sleep(period)


# ==============================================================================
# ZONE PIPELINE  -  one thread per physical zone (A1..B3): occupancy
# debounce (Section 19.5.5) -> 9-step CV -> shape-gate AND-fusion (Section 7.1)
# -> volume/mass (Section 8) -> servo dispatch (Section 12) -> PearData out.
# ==============================================================================
def zone_pipeline(zone: str, lane: int, rect, framebuf: FrameBuffer, state: SharedState,
                   servo: ServoController, out_queue: "queue.Queue[PearData]",
                   init_event: threading.Event, stop_event: threading.Event, cfg: dict):
    init_event.wait()   # Section 18: classical_vision_initialization must run first
    x1, y1, x2, y2 = rect
    vcfg = cfg["vision"]
    was_occupied = False
    last_publish = 0.0
    cooldown = cfg["servo"]["occupancy_cooldown_s"]
    is_row_a = zone.startswith("A")
    poll_s = cfg["timing"]["zone_poll_interval_s"]
    idle_s = cfg["timing"]["idle_sleep_s"]

    while not stop_event.is_set():
        color, depth, _fid = framebuf.get()
        if color is None:
            time.sleep(idle_s)
            continue
        zone_bgr = color[y1:y2, x1:x2]
        zone_depth = depth[y1:y2, x1:x2]
        if zone_bgr.size == 0:
            time.sleep(idle_s)
            continue

        cycle_start = time.time()
        result = run_classical_vision(zone_bgr, vcfg)
        occupied = result is not None
        now = time.time()

        # rising-edge occupancy + per-zone cooldown, Section 19.5.5
        # Continuous occupancy + per-zone cooldown
        if occupied and (now - last_publish) >= cooldown:
            state.set_active(zone, True)
            pear_id = state.next_pear_id(zone)
            if is_row_a:
                state.mark_row_a(lane)
                belt_speed = state.average_pear_speed() or 0.0
            else:
                belt_speed = state.mark_row_b(lane, cfg["zone_row_spacing_m"]) \
                             or state.average_pear_speed() or 0.0

            hsv_zone = cv2.cvtColor(zone_bgr, cv2.COLOR_BGR2HSV)
            is_pear_shape_ok = shape_gate_score(
                result["pear_contour"], result["pear_mask"], hsv_zone, SHAPE_GATE)
            infection_ratio = result["infection_ratio"]
            classical_bad = infection_ratio > vcfg["infection_ratio_threshold"]

            # Section 7.1 decision fusion: ACCEPT only if the shape/colour
            # gate AND the classical-vision infection check both say GOOD.
            accepted = is_pear_shape_ok and not classical_bad
            status = "ACCEPTED" if accepted else "REJECTED"

            volume_cm3, mass_g = estimate_volume_mass(
                zone_depth, result["pear_mask"], cfg["camera"]["working_distance_mm"],
                cfg["px_to_mm2"], cfg["mass_model"])
            category = "BIG" if result["surface_area_px"] >= cfg["big_small_threshold_px2"] else "SMALL"

            servo.dispatch(zone, accepted)
            state.set_active(zone, True, action=status)

            pear = PearData(
                pear_id=pear_id, zone=zone, status=status, category=category,
                infection_area_px=round(result["infection_area_px"], 1),
                infection_loc=tuple(round(v, 1) for v in result["infection_loc"]),
                infection_rgb=result["infection_rgb"],
                infection_ratio=round(infection_ratio, 4),
                surface_area_px=round(result["surface_area_px"], 1),
                volume_cm3=volume_cm3, mass_g=mass_g,
                belt_speed_m_s=round(belt_speed, 4),
            )
            out_queue.put(pear)
            state.record_result(status)

            elapsed = time.time() - cycle_start
            budget = cfg["action_cycle_budget_s"]
            flag = "" if elapsed <= budget else "  !! OVER 1s ACTION-CYCLE BUDGET !!"
            print(f"[{zone}] {pear_id} {status}/{category} ratio={infection_ratio:.3f} "
                  f"mass={mass_g:.1f}g  ({elapsed*1000:.0f} ms){flag}")

            last_publish = now
            state.set_active(zone, False, action=status)

        was_occupied = occupied
        time.sleep(poll_s)


# ==============================================================================
# SPEED CONTROLLER  -  Section 10.4 reference-speed algorithm + Section 10
# dual PID loops + Section 11.2 Conv1+Conv2 = 3.3 V invariant + Section 18
# max-6-pears overflow rule + Section 18.1 0.1 V floor. Drives the two real
# VoltageChannel software-PWM outputs (-> RC LPF -> PLC).
# ==============================================================================
class SpeedController:
    def __init__(self, cfg, state: SharedState, conv1: VoltageChannel, conv2: VoltageChannel):
        self.cfg = cfg
        self.state = state
        self.conv1 = conv1
        self.conv2 = conv2
        p1, p2 = cfg["pid_conv1"], cfg["pid_conv2"]
        self.pid_conv1 = PID(p1["kp"], p1["ki"], p1["kd"])
        self.pid_conv2 = PID(p2["kp"], p2["ki"], p2["kd"])
        self.min_v = cfg["speed_publisher"]["min_voltage"]
        self.max_v = cfg["speed_publisher"]["max_voltage"]
        self.max_speed = cfg["max_ref_speed_ms"]
        self.max_pears = cfg["max_pears_in_vision_zone"]
        self.trim_gain = cfg["speed_control"]["pid_trim_gain"]
        self.last_state = {"belt_state": "EMPTY", "conv1_v": self.max_v, "conv2_v": self.min_v,
                            "reference_speed_ms": 0.0, "pear_count": 0}

    def step(self, dt: float):
        pear_count = self.state.active_zone_count()

        if pear_count == 0:
            belt_state = "EMPTY"
            target_conv1, target_conv2 = self.max_v, self.min_v      # Table 17/27
        elif pear_count > self.max_pears:
            belt_state = "CROWDED"                                    # overflow, Section 18
            target_conv1, target_conv2 = self.min_v, self.max_v
        else:
            belt_state = "NORMAL"
            ref = self.state.average_pear_speed()
            ref_speed = ref if ref is not None else (pear_count / self.max_pears) * self.max_speed
            target_conv2 = (min(ref_speed, self.max_speed) / self.max_speed) * self.max_v
            target_conv1 = self.max_v - target_conv2                  # invariant, Section 11.2

        # PID trim around the target (measurement = last commanded value,
        # since no belt encoder hardware was supplied with this project -
        # this keeps the loop well-behaved without pretending to have a
        # sensor that doesn't exist; swap in real feedback here if/when a
        # conveyor encoder is added).
        conv1_v = target_conv1 + self.pid_conv1.step(
            target_conv1, self.last_state["conv1_v"], dt) * self.trim_gain
        conv1_v = min(max(conv1_v, self.min_v), self.max_v)
        conv2_v = min(max(self.max_v - conv1_v, self.min_v), self.max_v)  # re-derive: enforce sum
        conv1_v = self.max_v - conv2_v

        self.conv1.set_voltage(conv1_v)
        self.conv2.set_voltage(conv2_v)

        reference_speed_ms = (conv2_v / self.max_v) * self.max_speed if belt_state != "EMPTY" else 0.0
        self.last_state = {"belt_state": belt_state, "conv1_v": round(conv1_v, 3),
                            "conv2_v": round(conv2_v, 3),
                            "reference_speed_ms": round(reference_speed_ms, 4),
                            "pear_count": pear_count}
        return self.last_state


def speed_publisher_node(speed_ctrl: SpeedController, stop_event: threading.Event, cfg: dict):
    """Section 10.3: all three loops must settle well inside the 1 s cycle;
    this loop runs the PID/PWM update at 10 Hz (100 ms) by default, matching
    the ~10 ms budget line item of Table 20 for many updates per action
    cycle. Adjust CONFIG["speed_control"]["loop_dt_s"] to change the rate."""
    dt = cfg["speed_control"]["loop_dt_s"]
    while not stop_event.is_set():
        speed_ctrl.step(dt)
        time.sleep(dt)


# ==============================================================================
# DATA COLLECTION NODE  -  Section 14.1/18: strictly sequential DB writes
# A1 -> A2 -> A3 -> B1 -> B2 -> B3 -> repeat, plus packaging + IoT publish.
# ==============================================================================
class DataCollectionNode:
    def __init__(self, conn, state: SharedState, packaging: PackagingManager, cfg: dict):
        self.conn = conn
        self.state = state
        self.packaging = packaging
        self.cfg = cfg
        self.zone_queues = {z: queue.Queue() for z in ZONE_ORDER}
        self.last_record = None
        self.iot_status_json = "{}"
        self._iot_lock = threading.Lock()

    def queue_for(self, zone):
        return self.zone_queues[zone]

    def run(self, speed_ctrl: SpeedController, stop_event: threading.Event):
        iot_period = self.cfg["iot_publish_period_s"]
        idle_s = self.cfg["timing"]["idle_sleep_s"]
        last_iot = 0.0
        while not stop_event.is_set():
            wrote_any = False
            for z in ZONE_ORDER:                       # strict sequential order
                try:
                    pear: PearData = self.zone_queues[z].get_nowait()
                except queue.Empty:
                    continue
                wrote_any = True
                self._write_pear(pear)
                self.last_record = pear
                label = self.packaging.add_pear(pear)
                if label is not None:
                    self._write_package(label)
                    self.state.note_package_completed()
                    print(f"[data_collection_node] PACKAGE COMPLETE: {label.package_id} "
                          f"total={label.total_weight_g}g -> /labels/{label.package_id}")

            now = time.time()
            if now - last_iot >= iot_period:
                last_iot = now
                self._publish_iot(speed_ctrl)
            if not wrote_any:
                time.sleep(idle_s)

    def _write_pear(self, pear: PearData):
        self.conn.execute(
            """INSERT OR REPLACE INTO pear_records
               (pear_id, zone, timestamp, status, category, infection_area_px,
                infection_loc, infection_rgb, infection_ratio, surface_area_px,
                volume_cm3, mass_g, belt_speed_m_s)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (pear.pear_id, pear.zone, pear.timestamp, pear.status, pear.category,
             pear.infection_area_px, json.dumps(pear.infection_loc),
             json.dumps(pear.infection_rgb), pear.infection_ratio, pear.surface_area_px,
             pear.volume_cm3, pear.mass_g, pear.belt_speed_m_s))
        self.conn.commit()

    def _write_package(self, label: PackageLabel):
        self.conn.execute(
            """INSERT OR REPLACE INTO packages
               (package_id, start_timestamp, end_timestamp, duration_s,
                upper_layer, lower_layer, upper_weight_g, lower_weight_g, total_weight_g)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (label.package_id, label.start_time, label.end_time, label.duration_s,
             json.dumps(label.upper_layer), json.dumps(label.lower_layer),
             label.upper_weight_g, label.lower_weight_g, label.total_weight_g))
        self.conn.commit()

    def _publish_iot(self, speed_ctrl: SpeedController):
        """Section 14.3 / Table 23: the exact 13-field IoT schema."""
        belt = speed_ctrl.last_state
        payload = {
            "motors_status": self.state.motor_status_array(),                    # field 01
            "batch_rejections": self.state.batch_rejected,                       # field 02
            "batch_accepted": self.state.batch_accepted,                         # field 03
            "pear_id": self.last_record.pear_id if self.last_record else None,   # field 04
            "pear_status": self.last_record.status if self.last_record else None,  # 05
            "infection_area": self.last_record.infection_area_px if self.last_record else None,  # 06
            "infection_location": self.last_record.infection_loc if self.last_record else None,  # 07
            "infection_color": self.last_record.infection_rgb if self.last_record else None,  # 08
            "pear_surface_area": self.last_record.surface_area_px if self.last_record else None,  # 09
            "pear_volume": self.last_record.volume_cm3 if self.last_record else None,  # 10
            "pear_mass": self.last_record.mass_g if self.last_record else None,  # 11
            "pear_category": self.last_record.category if self.last_record else None,  # 12
            "completed_packages": self.state.completed_packages,                 # field 13
            "belt": belt,
            "timestamp": time.time(),
        }
        with self._iot_lock:
            self.iot_status_json = json.dumps(payload, default=str)

    def get_iot_status(self):
        with self._iot_lock:
            return self.iot_status_json


# ==============================================================================
# SCADA DASHBOARD  -  Section 15: Flask web publisher on port 8080.
# Dark IIoT theme, same visual language as the reference SCADA screenshots.
# ==============================================================================

# SAAT logo, embedded as base64 so the dashboard is one self-contained file
# with no extra static-asset folder to deploy alongside it. Served at
# /assets/logo.png by build_flask_app() and referenced by every page below.
LOGO_BASE64 = (
    "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"
)

# ------------------------------------------------------------------------------
# Shared top navigation bar (logo + page links as real buttons, Section: item 1)
# used by every dashboard page so navigation is consistent everywhere.
# ------------------------------------------------------------------------------
NAV_BAR = """
<div class="saat-nav">
  <a class="saat-nav-brand" href="/">
    <img src="/assets/logo.png" alt="SAAT logo">
    <span>SAAT <small>Agricultural Technology</small></span>
  </a>
  <div class="saat-nav-links">
    <a class="nav-btn {active_status}" href="/">Status</a>
    <a class="nav-btn {active_camera}" href="/camera">Camera</a>
    <a class="nav-btn {active_database}" href="/database">Database</a>
    <a class="nav-btn {active_labels}" href="/labels">Labels</a>
    <a class="nav-btn" href="/api/status" target="_blank">API</a>
  </div>
</div>
"""

NAV_BAR_CSS = """
  .saat-nav{display:flex;align-items:center;justify-content:space-between;
    flex-wrap:wrap;gap:12px;margin-bottom:20px;padding-bottom:16px;
    border-bottom:1px solid {{c.border}};}
  .saat-nav-brand{display:flex;align-items:center;gap:10px;text-decoration:none;color:inherit;}
  .saat-nav-brand img{height:38px;width:auto;display:block;}
  .saat-nav-brand span{font-size:16px;font-weight:bold;color:{{c.green}};letter-spacing:1px;}
  .saat-nav-brand span small{display:block;font-size:9px;font-weight:normal;color:#8b949e;letter-spacing:1.5px;}
  .saat-nav-links{display:flex;gap:8px;flex-wrap:wrap;}
  .nav-btn{background:{{c.surface}};border:1px solid {{c.border}};color:#c9d1d9;
    padding:8px 16px;border-radius:6px;font-size:12px;text-decoration:none;
    text-transform:uppercase;letter-spacing:0.5px;transition:all .15s ease;}
  .nav-btn:hover{border-color:{{c.blue}};color:{{c.blue}};}
  .nav-btn.active{background:{{c.blue}};border-color:{{c.blue}};color:#fff;}
  .btn{display:inline-flex;align-items:center;gap:6px;background:{{c.blue}};color:#fff;
    border:none;padding:9px 16px;border-radius:6px;font-size:12px;text-decoration:none;
    cursor:pointer;font-family:inherit;text-transform:uppercase;letter-spacing:0.5px;}
  .btn:hover{opacity:0.85;}
  .btn.secondary{background:{{c.surface}};border:1px solid {{c.border}};color:#c9d1d9;}
  .btn.green{background:{{c.green}};color:#04140c;}
  .btn.amber{background:{{c.amber}};color:#241a02;}
  .toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:4px;}
"""

STATUS_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="{{ refresh_s }}">
<title>SAAT SCADA - Status</title>
<style>
  *{box-sizing:border-box;}
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
""" + NAV_BAR_CSS + """
  .hero{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:18px;}
  .hero-title{font-size:13px;color:#8b949e;text-transform:uppercase;letter-spacing:1.5px;}
  .hw-pill{display:inline-flex;align-items:center;gap:6px;padding:5px 12px;border-radius:20px;font-size:11px;
    letter-spacing:0.5px;text-transform:uppercase;}
  .hw-pill.on{background:rgba(0,255,136,0.12);color:{{c.green}};border:1px solid {{c.green}};}
  .hw-pill.off{background:rgba(245,158,11,0.12);color:{{c.amber}};border:1px solid {{c.amber}};}
  .hw-pill .dot{width:7px;height:7px;border-radius:50%;background:currentColor;}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-top:16px;}
  .card{background:{{c.surface}};border:1px solid {{c.border}};border-radius:10px;padding:16px;
    box-shadow:0 1px 3px rgba(0,0,0,0.25);}
  .card h2{margin:0 0 10px 0;font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;
    display:flex;align-items:center;gap:6px;}
  .val{font-size:26px;font-weight:bold;}
  .accepted{color:{{c.green}};} .rejected{color:{{c.red}};}
  .amber{color:{{c.amber}};} .blue{color:{{c.blue}};}
  table{width:100%;border-collapse:collapse;margin-top:8px;font-size:13px;}
  td,th{border-bottom:1px solid {{c.border}};padding:6px 8px;text-align:left;}
  th{color:#8b949e;text-transform:uppercase;font-size:10px;letter-spacing:0.5px;}
  a{color:{{c.blue}};text-decoration:none;}
  .section-title{font-size:13px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px 0;}
  .footer{margin-top:20px;color:#484f58;font-size:11px;}
  .barwrap{background:#0b0f14;border-radius:6px;height:8px;overflow:hidden;margin-top:8px;border:1px solid {{c.border}};}
  .barfill{height:100%;border-radius:6px;}
</style></head><body>
""" + NAV_BAR.replace("{active_status}", "active").replace("{active_camera}", "") \
              .replace("{active_database}", "").replace("{active_labels}", "") + """

<div class="hero">
  <div class="hero-title">Live production status</div>
  <div class="hw-pill {{ 'on' if hardware_active else 'off' }}">
    <span class="dot"></span>{{ 'REAL HARDWARE' if hardware_active else 'OFFLINE / DEV MODE (simulated sensors)' }}
  </div>
</div>

<div class="grid">
  <div class="card"><h2>&#9881;&#65039; Belt State</h2><div class="val {{ 'accepted' if belt.belt_state=='NORMAL' else ('amber' if belt.belt_state=='EMPTY' else 'rejected') }}">{{ belt.belt_state or '-' }}</div></div>
  <div class="card"><h2>&#9889; Conv1 Voltage</h2><div class="val blue">{{ '%.3f'|format(belt.conv1_v or 0) }} V</div>
    <div class="barwrap"><div class="barfill" style="width:{{ ((belt.conv1_v or 0)/3.3*100)|round(1) }}%;background:{{c.blue}};"></div></div></div>
  <div class="card"><h2>&#9889; Conv2 Voltage</h2><div class="val blue">{{ '%.3f'|format(belt.conv2_v or 0) }} V</div>
    <div class="barwrap"><div class="barfill" style="width:{{ ((belt.conv2_v or 0)/3.3*100)|round(1) }}%;background:{{c.blue}};"></div></div></div>
  <div class="card"><h2>&#10003; Sum (must = 3.30 V)</h2><div class="val {{ 'accepted' if (belt.conv1_v or 0)+(belt.conv2_v or 0) > 3.25 else 'rejected' }}">{{ '%.3f'|format((belt.conv1_v or 0)+(belt.conv2_v or 0)) }} V</div></div>
  <div class="card"><h2>&#128664; Reference Speed</h2><div class="val">{{ '%.4f'|format(belt.reference_speed_ms or 0) }} m/s</div></div>
  <div class="card"><h2>&#127820; Pear Count (Vision Zone)</h2><div class="val">{{ belt.pear_count or 0 }} / 6</div>
    <div class="barwrap"><div class="barfill" style="width:{{ ((belt.pear_count or 0)/6*100)|round(1) }}%;background:{{c.amber}};"></div></div></div>
  <div class="card"><h2>&#9989; Accepted</h2><div class="val accepted">{{ batch_accepted }}</div></div>
  <div class="card"><h2>&#10060; Rejected</h2><div class="val rejected">{{ batch_rejected }}</div></div>
  <div class="card"><h2>&#128230; Completed Packages</h2><div class="val amber"><a href="/labels" style="color:inherit;">{{ completed_packages }}</a></div></div>
</div>

<div class="section-title">Motor / Zone Status</div>
<div class="card">
  <table><tr><th>Zone</th><th>Active</th><th>Last Action</th></tr>
  {% for m in motors_status %}
  <tr><td>{{m.zone}}</td><td>{{ 'YES' if m.active else 'no' }}</td>
      <td class="{{ 'accepted' if m.last_action=='ACCEPTED' else ('rejected' if m.last_action=='REJECTED' else '') }}">{{m.last_action}}</td></tr>
  {% endfor %}</table>
</div>

<div class="section-title">Last Pear Record</div>
<div class="card">
  {% if pear_id %}
  <table>
    <tr><td>pear_id</td><td>{{pear_id}}</td></tr>
    <tr><td>status</td><td class="{{ 'accepted' if pear_status=='ACCEPTED' else 'rejected' }}">{{pear_status}}</td></tr>
    <tr><td>category</td><td>{{pear_category}}</td></tr>
    <tr><td>infection_area (px^2)</td><td>{{infection_area}}</td></tr>
    <tr><td>infection_location</td><td>{{infection_location}}</td></tr>
    <tr><td>infection_color (RGB)</td><td>{{infection_color}}</td></tr>
    <tr><td>surface_area (px^2)</td><td>{{pear_surface_area}}</td></tr>
    <tr><td>volume (cm^3)</td><td>{{pear_volume}}</td></tr>
    <tr><td>mass (g)</td><td>{{pear_mass}}</td></tr>
  </table>
  {% else %}<div>No pears processed yet...</div>{% endif %}
</div>

<div class="footer">
  auto-refresh every {{ refresh_s }}s &nbsp;|&nbsp; updated {{ now }}
</div>
</body></html>
"""

DATABASE_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT SCADA - Database</title>
<style>
  *{box-sizing:border-box;}
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
""" + NAV_BAR_CSS + """
  h1{color:{{c.green}};font-size:18px;margin:0 0 4px 0;}
  .sub{color:#8b949e;font-size:12px;margin-bottom:14px;}
  .card{background:{{c.surface}};border:1px solid {{c.border}};border-radius:10px;padding:14px;overflow-x:auto;}
  table{width:100%;border-collapse:collapse;font-size:12px;}
  td,th{border-bottom:1px solid {{c.border}};padding:6px 8px;text-align:left;white-space:nowrap;}
  th{color:#8b949e;text-transform:uppercase;font-size:11px;position:sticky;top:0;background:{{c.surface}};}
  .ACCEPTED{color:{{c.green}};} .REJECTED{color:{{c.red}};}
  a{color:{{c.blue}};}
</style></head><body>
""" + NAV_BAR.replace("{active_status}", "").replace("{active_camera}", "") \
              .replace("{active_database}", "active").replace("{active_labels}", "") + """

<h1>&#128190; pear_records</h1>
<div class="sub">Showing the {{ row_limit }} most recent rows. Use the buttons below to export the full table.</div>

<div class="toolbar" style="margin-bottom:16px;">
  <a class="btn green" href="/download/database/all.xlsx">&#11015;&#65039; Download All (Excel)</a>
  <a class="btn amber" href="/download/database/today.xlsx">&#128197; Download Today (Excel)</a>
</div>

<div class="card">
<table><tr>{% for col in columns %}<th>{{col}}</th>{% endfor %}</tr>
{% for row in rows %}<tr>{% for i in range(row|length) %}
<td class="{{ row[3] if i==3 else '' }}">{{ row[i] }}</td>{% endfor %}</tr>{% endfor %}
</table>
</div>
</body></html>
"""

CAMERA_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT - Live Camera</title>
<style>
  *{box-sizing:border-box;}
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
""" + NAV_BAR_CSS + """
  h1{color:{{c.green}};font-size:18px;letter-spacing:1px;margin:0 0 4px 0;}
  p{color:#8b949e;font-size:12px;}
  a{color:{{c.blue}};text-decoration:none;}
  .cam-wrap{display:flex;flex-direction:column;align-items:center;margin-top:8px;}
  .frame{max-width:900px;width:100%;border:1px solid {{c.border}};border-radius:10px;background:#000;
    box-shadow:0 4px 18px rgba(0,0,0,0.35);}
  .legend{margin-top:14px;font-size:12px;color:#8b949e;display:flex;gap:20px;}
  .swatch{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;vertical-align:middle;}
  .details-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:16px;margin-top:28px;}
  .card{background:{{c.surface}};border:1px solid {{c.border}};border-radius:10px;padding:16px;}
  .card h2{margin:0 0 10px 0;font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;}
  .card table{width:100%;font-size:12px;border-collapse:collapse;}
  .card td{padding:4px 0;border-bottom:1px solid {{c.border}};}
  .card td:last-child{text-align:right;color:{{c.blue}};font-weight:bold;}
  .section-title{font-size:13px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin:0 0 8px 0;text-align:center;}
</style></head><body>
""" + NAV_BAR.replace("{active_status}", "").replace("{active_camera}", "active") \
              .replace("{active_database}", "").replace("{active_labels}", "") + """

<h1>&#128247; What The Camera Sees</h1>
<p>Live zone-annotated feed from the Intel RealSense D455, ~{{ fps }} fps &nbsp;|&nbsp; {{ 'REAL HARDWARE' if hardware_active else 'OFFLINE / DEV MODE (simulated frames)' }}</p>

<div class="cam-wrap">
  <img class="frame" src="/video_feed">
  <div class="legend">
    <div><span class="swatch" style="background:#8c8c8c;"></span>idle / no pear</div>
    <div><span class="swatch" style="background:{{c.green}};"></span>accepted</div>
    <div><span class="swatch" style="background:{{c.red}};"></span>rejected</div>
  </div>
</div>

<div class="section-title" style="margin-top:32px;">System Details</div>
<div class="details-grid">
  <div class="card">
    <h2>&#128247; Camera</h2>
    <table>
      <tr><td>Colour stream</td><td>{{cam.color_width}}x{{cam.color_height}} @ {{cam.color_fps}} fps</td></tr>
      <tr><td>Depth stream</td><td>{{cam.depth_width}}x{{cam.depth_height}} @ {{cam.depth_fps}} fps</td></tr>
      <tr><td>Working distance</td><td>{{cam.working_distance_mm}} mm</td></tr>
    </table>
  </div>
  <div class="card">
    <h2>&#127820; Vision Thresholds</h2>
    <table>
      <tr><td>Infection ratio limit</td><td>{{ (vision.infection_ratio_threshold*100)|round(1) }}%</td></tr>
      <tr><td>Min pear area</td><td>{{vision.min_pear_area_px}} px&sup2;</td></tr>
      <tr><td>BIG / SMALL cutoff</td><td>{{big_small_threshold}} px&sup2;</td></tr>
      <tr><td>Shape checks required</td><td>{{shape_min_checks}} / 4</td></tr>
    </table>
  </div>
  <div class="card">
    <h2>&#8987; Timing</h2>
    <table>
      <tr><td>Action-cycle budget</td><td>{{action_cycle_budget}} s</td></tr>
      <tr><td>Zone occupancy cooldown</td><td>{{occupancy_cooldown}} s</td></tr>
      <tr><td>Max pears in vision zone</td><td>{{max_pears}}</td></tr>
    </table>
  </div>
  <div class="card">
    <h2>&#9881;&#65039; Zones (A1-B3)</h2>
    <table>
      {% for m in motors_status %}
      <tr><td>{{m.zone}}</td><td class="{{ 'accepted' if m.last_action=='ACCEPTED' else ('rejected' if m.last_action=='REJECTED' else '') }}" style="color:inherit;">{{m.last_action}}</td></tr>
      {% endfor %}
    </table>
  </div>
</div>
</body></html>
"""

LABELS_INDEX_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT SCADA - Package Labels</title>
<style>
  *{box-sizing:border-box;}
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
""" + NAV_BAR_CSS + """
  h1{color:{{c.green}};font-size:18px;margin:0 0 4px 0;}
  .sub{color:#8b949e;font-size:12px;margin-bottom:14px;}
  .card{background:{{c.surface}};border:1px solid {{c.border}};border-radius:10px;padding:14px;overflow-x:auto;}
  table{width:100%;border-collapse:collapse;font-size:13px;}
  td,th{border-bottom:1px solid {{c.border}};padding:8px 10px;text-align:left;}
  th{color:#8b949e;text-transform:uppercase;font-size:11px;}
  a{color:{{c.blue}};text-decoration:none;}
  a:hover{text-decoration:underline;}
  .row-btn{display:inline-block;padding:5px 10px;border-radius:5px;font-size:11px;text-transform:uppercase;
    letter-spacing:0.5px;border:1px solid {{c.border}};background:{{c.bg}};margin-right:6px;}
  .row-btn:hover{border-color:{{c.blue}};}
  .empty{color:#8b949e;margin-top:16px;}
</style></head><body>
""" + NAV_BAR.replace("{active_status}", "").replace("{active_camera}", "") \
              .replace("{active_database}", "").replace("{active_labels}", "active") + """

<h1>&#127991; Package Labels</h1>
<div class="sub">Completed packages (12 BIG + 12 SMALL each). Click a package to view or download its printable label.</div>

{% if packages %}
<div class="toolbar" style="margin-bottom:16px;">
  <a class="btn green" href="/download/labels/all.csv">&#11015;&#65039; Download All Labels (CSV)</a>
</div>
<div class="card">
<table>
<tr><th>Package ID</th><th>Completed At</th><th>Packaging Clock</th><th>Upper (BIG) g</th><th>Lower (SMALL) g</th><th>Total g</th><th></th></tr>
{% for p in packages %}
<tr>
  <td>{{p.package_id}}</td><td>{{p.completed_at}}</td><td>{{p.duration}}</td>
  <td>{{p.upper_weight_g}}</td><td>{{p.lower_weight_g}}</td><td>{{p.total_weight_g}}</td>
  <td>
    <a class="row-btn" href="/labels/{{p.package_id}}">View / Print</a>
    <a class="row-btn" href="/download/labels/{{p.package_id}}.csv">Download</a>
  </td>
</tr>
{% endfor %}
</table>
</div>
{% else %}
<div class="empty">No packages completed yet - each package needs 12 BIG + 12 SMALL accepted pears.</div>
{% endif %}
</body></html>
"""

LABEL_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>Label - {{package_id}}</title>
<style>
  *{box-sizing:border-box;}
  body{background:#e9edf1;color:#111;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  .toolbar-row{width:980px;margin:0 auto 16px auto;display:flex;justify-content:space-between;align-items:center;}
  .backlink{color:#3b82f6;text-decoration:none;font-size:13px;}
  .print-btn{background:#111;color:#fff;border:none;padding:9px 18px;border-radius:6px;font-size:12px;
    text-transform:uppercase;letter-spacing:0.5px;cursor:pointer;font-family:inherit;}
  .print-btn:hover{opacity:0.85;}
  .label-master{background:#fff;border:3px solid #111;width:980px;margin:0 auto 32px auto;}
  .mh-top{border-bottom:3px solid #111;padding:10px 16px;display:flex;align-items:center;justify-content:flex-end;gap:10px;}
  .mh-top img{height:34px;width:auto;}
  .mh-top span{font-size:22px;font-weight:bold;letter-spacing:2px;}
  .mh-body{display:grid;grid-template-columns:1fr 1fr 260px;}
  .layer-col{padding:16px;border-right:2px solid #111;}
  .info-col{padding:16px;display:flex;flex-direction:column;gap:14px;align-items:center;}
  .info-col img{height:56px;width:auto;margin-bottom:2px;}
  .layer-title{text-align:center;font-weight:bold;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;}
  .pear-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:4px;}
  .pear-cell{border:1px solid #111;padding:8px 2px;text-align:center;font-size:11px;line-height:1.4;}
  .pear-cell b{display:block;font-size:12px;}
  .layer-total{text-align:center;margin-top:10px;font-weight:bold;font-size:13px;}
  .info-row{width:100%;text-align:center;}
  .info-row .lbl{display:block;font-size:10px;color:#555;text-transform:uppercase;letter-spacing:1px;}
  .info-row .val{display:block;font-size:14px;font-weight:bold;margin-top:2px;}
  h2.section{width:980px;margin:24px auto 8px auto;font-size:15px;color:#333;}
  .pear-labels-grid{width:980px;margin:0 auto;display:flex;flex-wrap:wrap;gap:10px;}
  .pear-label{display:flex;width:230px;height:60px;border:2px solid #111;background:#fff;}
  .pear-label img{height:28px;width:auto;align-self:center;margin-left:8px;}
  .pear-label .info{flex:1;padding:6px 8px;display:flex;flex-direction:column;justify-content:center;font-size:11px;gap:4px;}
  .pear-label .info b{font-size:12px;}
  .cat-BIG{border-left:6px solid #3b82f6;}
  .cat-SMALL{border-left:6px solid #f59e0b;}
  @media print {
    .toolbar-row{display:none;}
    body{background:#fff;padding:0;}
  }
</style></head><body>
<div class="toolbar-row">
  <a class="backlink" href="/labels">&larr; back to package list</a>
  <div>
    <a class="print-btn" href="/download/labels/{{package_id}}.csv" style="text-decoration:none;display:inline-block;margin-right:8px;">&#11015;&#65039; Download CSV</a>
    <button class="print-btn" onclick="window.print()">&#128424;&#65039; Print / Save PDF</button>
  </div>
</div>
<div class="label-master">
  <div class="mh-top"><img src="/assets/logo.png" alt="SAAT logo"><span>SAAT</span></div>
  <div class="mh-body">
    <div class="layer-col">
      <div class="layer-title">Upper Layer (BIG)</div>
      <div class="pear-grid">{% for cell in upper %}<div class="pear-cell">{{cell.position}}<b>{{cell.pear_id}}</b></div>{% endfor %}</div>
      <div class="layer-total">Total Mass: {{upper_weight_g}} g</div>
    </div>
    <div class="layer-col">
      <div class="layer-title">Lower Layer (SMALL)</div>
      <div class="pear-grid">{% for cell in lower %}<div class="pear-cell">{{cell.position}}<b>{{cell.pear_id}}</b></div>{% endfor %}</div>
      <div class="layer-total">Total Mass: {{lower_weight_g}} g</div>
    </div>
    <div class="info-col">
      <img src="/assets/logo.png" alt="SAAT logo">
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
    <img src="/assets/logo.png" alt="SAAT">
    <div class="info"><div>Package: <b>{{package_id}}</b></div><div>Pear ID: <b>{{item.pear_id}}</b></div></div>
  </div>
  {% endfor %}
</div>
</body></html>
"""


def build_flask_app(dc_node: DataCollectionNode, speed_ctrl: SpeedController,
                     db_path: Path, camera_hardware_active: bool, cfg: dict,
                     stream_buf: "VideoStreamBuffer"):
    app = Flask(__name__)
    dash = cfg["dashboard"]
    colors = dash["colors"]

    @app.route("/")
    def status():
        raw = dc_node.get_iot_status()
        payload = json.loads(raw) if raw and raw != "{}" else {}
        return render_template_string(
            STATUS_PAGE, c=colors, refresh_s=dash["auto_refresh_s"],
            belt=payload.get("belt", {}) or {},
            motors_status=payload.get("motors_status", []),
            batch_accepted=payload.get("batch_accepted", 0),
            batch_rejected=payload.get("batch_rejections", 0),
            completed_packages=payload.get("completed_packages", 0),
            pear_id=payload.get("pear_id"), pear_status=payload.get("pear_status"),
            pear_category=payload.get("pear_category"),
            infection_area=payload.get("infection_area"),
            infection_location=payload.get("infection_location"),
            infection_color=payload.get("infection_color"),
            pear_surface_area=payload.get("pear_surface_area"),
            pear_volume=payload.get("pear_volume"), pear_mass=payload.get("pear_mass"),
            hardware_active=camera_hardware_active,
            now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @app.route("/database")
    def database():
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute("SELECT * FROM pear_records ORDER BY timestamp DESC LIMIT ?",
                            (dash["database_rows_shown"],))
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
        conn.close()
        return render_template_string(DATABASE_PAGE, c=colors, columns=columns, rows=rows,
                                       row_limit=dash["database_rows_shown"])

    @app.route("/api/status")
    def api_status():
        raw = dc_node.get_iot_status()
        return jsonify(json.loads(raw) if raw else {})

    @app.route("/camera")
    def camera_view():
        return render_template_string(CAMERA_PAGE, c=colors, fps=cfg["camera_stream"]["target_fps"])

    @app.route("/video_feed")
    def video_feed():
        def generate():
            idle_wait = 1.0 / max(cfg["camera_stream"]["target_fps"], 1)
            while True:
                frame = stream_buf.get_frame()
                if frame is None:
                    time.sleep(idle_wait)
                    continue
                yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + frame + b"\r\n")
                time.sleep(idle_wait)
        return Response(generate(), mimetype="multipart/x-mixed-replace; boundary=frame")

    @app.route("/labels")
    def labels_index():
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute(
            "SELECT package_id, end_timestamp, duration_s, upper_weight_g, "
            "lower_weight_g, total_weight_g FROM packages ORDER BY end_timestamp DESC")
        rows = cur.fetchall()
        conn.close()
        packages = [{"package_id": r[0],
                     "completed_at": datetime.fromtimestamp(r[1]).strftime("%Y-%m-%d %H:%M:%S"),
                     "duration": f"{r[2]:.1f}s", "upper_weight_g": r[3],
                     "lower_weight_g": r[4], "total_weight_g": r[5]} for r in rows]
        return render_template_string(LABELS_INDEX_PAGE, c=colors, packages=packages)

    @app.route("/labels/<package_id>")
    def label_detail(package_id):
        conn = sqlite3.connect(str(db_path))
        cur = conn.execute(
            "SELECT package_id, start_timestamp, end_timestamp, duration_s, upper_layer, "
            "lower_layer, upper_weight_g, lower_weight_g, total_weight_g "
            "FROM packages WHERE package_id = ?", (package_id,))
        row = cur.fetchone()
        conn.close()
        if row is None:
            return f"Package {package_id} not found. <a href='/labels'>Back</a>", 404
        (_pid, start_ts, end_ts, duration_s, upper_json, lower_json,
         upper_w, lower_w, total_w) = row
        upper, lower = json.loads(upper_json), json.loads(lower_json)
        all_pears = ([{"pear_id": c["pear_id"], "category": "BIG"} for c in upper] +
                     [{"pear_id": c["pear_id"], "category": "SMALL"} for c in lower])
        mins, secs = divmod(int(duration_s), 60)
        return render_template_string(
            LABEL_PAGE, package_id=package_id, upper=upper, lower=lower,
            upper_weight_g=upper_w, lower_weight_g=lower_w, total_weight_g=total_w,
            all_pears=all_pears,
            packaging_time=datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S"),
            packaging_clock=f"{mins:02d}:{secs:02d}")

    return app


# ==============================================================================
# LAUNCH  -  staggered startup, Section 16.4: classical_vision_initialization
# must complete before any vision node runs.
# ==============================================================================
def classical_vision_initialization(init_event: threading.Event, cfg: dict):
    """Layer 0: broadcasts shared vision parameters, then opens the gate for
    every zone_pipeline thread (Section 18: 'must run before any vision node')."""
    print("[classical_vision_initialization] thresholds:",
          {k: cfg["vision"][k] for k in ("infection_ratio_threshold", "min_pear_area_px")})
    init_event.set()


def main():
    rd = RUNTIME_DEFAULTS   # edit RUNTIME_DEFAULTS at the top of the file to change these
    ap = argparse.ArgumentParser(description="SAAT - unified vision + control production system")
    ap.add_argument("--port", type=int, default=rd["port"])
    ap.add_argument("--no-web", action="store_true", default=rd["no_web"],
                     help="headless: skip the Flask dashboard")
    ap.add_argument("--duration", type=float, default=rd["duration"],
                     help="auto-stop after N seconds (0 = run until Ctrl+C)")
    ap.add_argument("--db-path", type=str, default=rd["db_path"])
    ap.add_argument("--force-sim", action="store_true", default=rd["force_sim"],
                     help="ignore any hardware libraries even if present (dev/demo mode)")
    args = ap.parse_args()

    global HAVE_REALSENSE, HAVE_GPIO, HAVE_SERVOKIT
    if args.force_sim:
        HAVE_REALSENSE = HAVE_GPIO = HAVE_SERVOKIT = False
        print("[SAAT] --force-sim: all hardware paths disabled, running fully offline.")

    if not HAVE_FLASK and not args.no_web:
        print("Flask is not installed. Install it with:\n    pip install flask\n"
              "or re-run with --no-web to run headless.")
        return

    print("=" * 78)
    print(" SAAT PRODUCTION SYSTEM - Pear Sorting & Packaging Line")
    print(" Vision Subsystem + Control Subsystem, unified single-process launch")
    print("=" * 78)

    state = SharedState()
    stop_event = threading.Event()
    init_event = threading.Event()
    db_path = Path(args.db_path)
    conn = init_db(db_path)
    packaging = PackagingManager(CONFIG["packaging"]["big_per_package"],
                                  CONFIG["packaging"]["small_per_package"])
    dc_node = DataCollectionNode(conn, state, packaging, CONFIG)

    threads = []

    # T+0: classical_vision_initialization (Section 18: hard startup ordering)
    print("[SAAT] T+0: classical_vision_initialization...")
    classical_vision_initialization(init_event, CONFIG)

    # T+1: camera + frame_capture_node
    print("[SAAT] T+1: starting camera / frame_capture_node...")
    camera = CameraSystem(CONFIG)
    framebuf = FrameBuffer()
    t = threading.Thread(target=frame_capture_node, args=(camera, framebuf, stop_event, CONFIG),
                          daemon=True)
    t.start(); threads.append(t)
    time.sleep(0.3)

    # T+2: hardware I/O - servo controller + the two conveyor voltage channels
    print("[SAAT] T+2: starting ServoController + VoltageChannels (Conv1/Conv2)...")
    servo = ServoController(CONFIG["servo"])
    sp_cfg = CONFIG["speed_publisher"]
    conv1 = VoltageChannel(sp_cfg["gpio_pin_conv1"], sp_cfg["pwm_frequency_hz"],
                            sp_cfg["max_voltage"], "Conv1")
    conv2 = VoltageChannel(sp_cfg["gpio_pin_conv2"], sp_cfg["pwm_frequency_hz"],
                            sp_cfg["max_voltage"], "Conv2")
    speed_ctrl = SpeedController(CONFIG, state, conv1, conv2)

    # T+3: six zone pipelines (A1..B3) - one thread per physical zone
    print("[SAAT] T+3: starting 6 zone pipelines (A1..B3)...")
    grid = CONFIG["zone_grid"]
    crop_w = grid["crop_x2"] - grid["crop_x1"]
    crop_h = grid["crop_y2"] - grid["crop_y1"]
    rects = zone_rectangles(crop_w, crop_h, grid)
    for zone, rect in rects.items():
        lane = int(zone[1])
        t = threading.Thread(target=zone_pipeline,
                              args=(zone, lane, rect, framebuf, state, servo,
                                    dc_node.queue_for(zone), init_event, stop_event, CONFIG),
                              daemon=True)
        t.start(); threads.append(t)

    # T+4: speed_publisher_node (dual PID -> PWM -> LPF -> PLC)
    print("[SAAT] T+4: starting speed_publisher_node...")
    t = threading.Thread(target=speed_publisher_node, args=(speed_ctrl, stop_event, CONFIG), daemon=True)
    t.start(); threads.append(t)

    # T+5: data_collection_node (sequential DB writer + packaging + IoT publish)
    print("[SAAT] T+5: starting data_collection_node...")
    t = threading.Thread(target=dc_node.run, args=(speed_ctrl, stop_event), daemon=True)
    t.start(); threads.append(t)

    # T+5.5: live camera view ("what the camera sees, and what it does")
    stream_buf = VideoStreamBuffer()
    if CONFIG["camera_stream"]["enabled"]:
        print("[SAAT] T+5.5: starting video_stream_node (live /camera view)...")
        t = threading.Thread(target=video_stream_node,
                              args=(framebuf, state, rects, stream_buf, stop_event, CONFIG),
                              daemon=True)
        t.start(); threads.append(t)

    # T+6: SCADA dashboard
    if not args.no_web:
        app = build_flask_app(dc_node, speed_ctrl, db_path, camera.hardware_active, CONFIG, stream_buf)
        t = threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=args.port, debug=False,
                                    use_reloader=False, threaded=True), daemon=True)
        t.start(); threads.append(t)
        time.sleep(0.5)
        print(f"[SAAT] Dashboard : http://localhost:{args.port}")
        print(f"[SAAT] Live Camera: http://localhost:{args.port}/camera")
        print(f"[SAAT] Database  : http://localhost:{args.port}/database")
        print(f"[SAAT] Labels    : http://localhost:{args.port}/labels")
        print(f"[SAAT] JSON API  : http://localhost:{args.port}/api/status")
    else:
        print("[SAAT] --no-web: dashboard disabled (headless mode).")

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
        conv1.stop(); conv2.stop()
        if HAVE_GPIO:
            GPIO.cleanup()
        camera.stop()
        conn.close()
        print("[SAAT] Stopped. Database saved at:", db_path.resolve())


if __name__ == "__main__":
    main()
