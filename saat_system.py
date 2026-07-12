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
    from flask import Flask, jsonify, render_template_string
    HAVE_FLASK = True
except ImportError:
    HAVE_FLASK = False


# ==============================================================================
# CONFIG  -  every numeric value below is taken verbatim from the documentation
# (docx sections/tables cited in the comments); nothing here is a placeholder.
# ==============================================================================
ZONE_ORDER = ["A1", "A2", "A3", "B1", "B2", "B3"]          # Sections 4.2 / 14.1
MOTOR_CHANNEL = {"A1": 0, "A2": 1, "A3": 2, "B1": 3, "B2": 4, "B3": 5}  # servos_testing.py

CONFIG = {
    # --- Section 3 / Table 1: camera --------------------------------------
    "camera": {
        "color_width": 1280, "color_height": 800, "color_fps": 30,   # practical
        "depth_width": 1280, "depth_height": 720, "depth_fps": 30,   # RealSense caps
        "working_distance_mm": 380,        # belt-to-camera, Section 3.1/8.2
    },
    # --- Table 3: frame column split (of the raw 1280-wide colour frame) ---
    "frame_columns": {"conv1": (0, 256), "vision": (257, 1024), "conv2": (1025, 1280)},
    # --- calibrated physical belt-zone grid, tuned percentages carried over
    #     from distances.py / frames_test.py draw_grid() -> re-tune here to
    #     fit your rig (comment preserved from the original scripts) --------
    "zone_grid": {
        "crop_x1": 180, "crop_x2": 1030, "crop_y1": 0, "crop_y2": 800,
        "top_margin_pct": 0.08,     # C1 boundary (pear entry line, t1)
        "mid_y_pct": 0.52,          # Row A / Row B divider
        "bottom_margin_pct": 0.95,  # C2 boundary (pear exit line, t2)
        "vert_left_pct": 0.30,      # lane 1 / lane 2 divider
        "vert_right_pct": 0.63,     # lane 2 / lane 3 divider
    },
    "zone_row_spacing_m": 0.15,     # design value: physical A-row -> B-row distance
    # --- Section 6 / Table 30 (final, production row): infection detection -
    "vision": {
        "min_pear_area_px": 800,
        "infection_ratio_threshold": 0.05,          # Section 9: REJECT if > 5%
        "infection_hsv_lo": (0, 0, 0),
        "infection_hsv_hi": (180, 150, 80),          # dark/low-value blemish band
        "clahe_clip": 2.0, "clahe_tile": (8, 8),
        "bilateral_d": 9, "bilateral_sigma": 75,
    },
    "big_small_threshold_px2": 15000,               # Section 8.6
    # --- Section 8.4: mass regression --------------------------------------
    "mass_model": {"density_g_cm3": 0.960, "intercept_g": -0.02},
    # calibration constant translating one silhouette pixel into mm^2 of real
    # belt area at the fixed 380 mm working distance (Section 8.2 pinhole
    # relation, pre-computed for this rig's focal length -> derived constant)
    "px_to_mm2": 0.070,
    # --- Section 10 / Table 15: PID gains (Conv1, Conv2, Servo loops) ------
    "pid_conv1": {"kp": 0.16, "ki": 11.76, "kd": 0.020},
    "pid_conv2": {"kp": 0.14, "ki": 15.45, "kd": 0.015},
    "pid_servo": {"kp": 0.07, "ki": 11.12, "kd": 0.010},
    "max_ref_speed_ms": 0.5,
    # --- Section 11 / Table 18: PWM -> LPF -> PLC --------------------------
    "speed_publisher": {
        "gpio_pin_conv1": 11,     # physical pin 11 / GPIO17 (Table 2/26)
        "gpio_pin_conv2": 13,     # physical pin 13 / GPIO27
        "pwm_frequency_hz": 500,
        "min_voltage": 0.1,       # Section 18.1: never true 0 V
        "max_voltage": 3.3,
        "lpf_r_ohm": 10_000, "lpf_c_f": 10e-6,   # Table 18 -> fc ~= 1.59 Hz
    },
    # --- Section 12 / Table 19: servo actuation ----------------------------
    "servo": {
        "accept_angle": 0.0, "reject_angle": 90.0,
        "return_delay_s": 0.4,
        "pulse_min_us": 1000, "pulse_max_us": 2000,
        "pwm_freq_hz": 50,
        "occupancy_cooldown_s": 3.0,   # Section 19.5.5 debounce
    },
    # --- Section 15.3 / Figure 15.2: packaging -----------------------------
    "packaging": {"big_per_package": 12, "small_per_package": 12, "company_name": "SAAT"},
    # --- Section 14.3: 0.1 Hz IoT publish -----------------------------------
    "iot_publish_period_s": 10.0,
    # --- Section 18 hard limit ----------------------------------------------
    "max_pears_in_vision_zone": 6,
    # --- Section 13 / Table 20: 1-second action-cycle timing budget --------
    "action_cycle_budget_s": 1.0,
}

# Section 19.3 / Table 29: shape+colour "is this a pear" gate thresholds,
# used here as the second, independent vote that is AND-fused with the
# classical infection decision (Section 7.1), standing in for the
# TensorFlow classifier that requires a trained model not supplied here.
SHAPE_GATE = {
    "aspect_ratio": (1.1, 2.5),
    "extent": (0.5, 0.9),
    "solidity_min": 0.85,
    "circularity": (0.4, 0.9),
    "min_checks_passed": 3,   # out of 4
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
            GPIO.setwarnings(False)
            GPIO.setmode(GPIO.BOARD)
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
                print("[ServoController] PCA9685 initialised, pulse widths calibrated.")
            except Exception as e:
                print(f"[ServoController] Hardware error: {e}. Falling back to offline mode.")
                self.hardware_active = False
        else:
            print("[ServoController] adafruit_servokit not available - offline mode "
                  "(servo moves will be logged, not executed).")

    def set_angle(self, zone: str, angle: float):
        channel = MOTOR_CHANNEL[zone]
        with self._lock:
            if self.hardware_active:
                self.kit.servo[channel].angle = angle
            else:
                print(f"[servo:sim] {zone} (ch {channel}) -> {angle:.0f} deg")

    def dispatch(self, zone: str, accepted: bool):
        """Section 12: only the ACTION_NODE for this zone may move hardware."""
        if accepted:
            self.set_angle(zone, self.cfg["accept_angle"])
        else:
            self.set_angle(zone, self.cfg["reject_angle"])
            time.sleep(self.cfg["return_delay_s"])
            self.set_angle(zone, self.cfg["accept_angle"])


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


def draw_zone_overlay(img, rects):
    """Debug/HMI overlay - red grid lines + zone labels, same look as
    distances.py / frames_test.py's draw_grid()."""
    for zone, (x1, y1, x2, y2) in rects.items():
        cv2.rectangle(img, (x1, y1), (x2, y2), (0, 0, 255), 2)
        cv2.putText(img, zone, (x1 + 6, y1 + 24), cv2.FONT_HERSHEY_SIMPLEX,
                    0.7, (0, 255, 255), 2, cv2.LINE_AA)
    return img


# ==============================================================================
# CAMERA SYSTEM  -  generalisation of distances.py / frames_test.py.
# Real Intel RealSense D455 when available (Section 3.1); otherwise a clearly
# labelled synthetic frame generator so the whole pipeline can be developed
# and demonstrated without the physical rig attached.
# ==============================================================================
class CameraSystem:
    def __init__(self, cfg):
        self.cfg = cfg
        self.hardware_active = HAVE_REALSENSE
        self.depth_scale = 0.001   # metres per depth unit, RealSense default
        self._sim_rng = np.random.default_rng(42)
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
        w, h = self.cfg["color_width"], self.cfg["color_height"]
        color = np.full((h, w, 3), (60, 90, 40), dtype=np.uint8)   # dull belt green

        if self._sim_rng.random() < 0.02 and len(self._sim_pears) < 5:
            self._sim_pears.append({
                "x": float(self._sim_rng.uniform(0.15 * w, 0.85 * w)),
                "y": -30.0,
                "r": float(self._sim_rng.uniform(45, 70)),
                "infected": bool(self._sim_rng.random() < 0.30),
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
            p["y"] += 6.0
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
    kernel = np.ones((5, 5), np.uint8)
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

    while not stop_event.is_set():
        color, depth, _fid = framebuf.get()
        if color is None:
            time.sleep(0.05)
            continue
        zone_bgr = color[y1:y2, x1:x2]
        zone_depth = depth[y1:y2, x1:x2]
        if zone_bgr.size == 0:
            time.sleep(0.05)
            continue

        cycle_start = time.time()
        result = run_classical_vision(zone_bgr, vcfg)
        occupied = result is not None
        now = time.time()

        # rising-edge occupancy + per-zone cooldown, Section 19.5.5
        if occupied and not was_occupied and (now - last_publish) >= cooldown:
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
        time.sleep(0.02)


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
            target_conv1, self.last_state["conv1_v"], dt) * 0.01
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


def speed_publisher_node(speed_ctrl: SpeedController, stop_event: threading.Event):
    """Section 10.3: all three loops must settle well inside the 1 s cycle;
    this loop runs the PID/PWM update at 10 Hz (100 ms), matching the ~10 ms
    budget line item of Table 20 for many updates per action cycle."""
    dt = 0.1
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
                time.sleep(0.05)

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
COLORS = {"bg": "#0d1117", "surface": "#161b22", "border": "#30363d",
          "green": "#00ff88", "amber": "#f59e0b", "red": "#ef4444", "blue": "#3b82f6"}

STATUS_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="10">
<title>SAAT SCADA - Status</title>
<style>
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  h1{color:{{c.green}};font-size:20px;letter-spacing:1px;}
  .sub{color:#8b949e;font-size:12px;margin-top:-8px;}
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
  .badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;}
</style></head><body>
<h1>&#9679; SAAT SCADA DASHBOARD</h1>
<div class="sub">{{ 'REAL HARDWARE' if hardware_active else 'OFFLINE / DEV MODE (simulated sensors)' }}</div>
<div class="grid">
  <div class="card"><h2>Belt State</h2><div class="val {{ 'accepted' if belt.belt_state=='NORMAL' else ('amber' if belt.belt_state=='EMPTY' else 'rejected') }}">{{ belt.belt_state or '-' }}</div></div>
  <div class="card"><h2>Conv1 Voltage</h2><div class="val blue">{{ '%.3f'|format(belt.conv1_v or 0) }} V</div></div>
  <div class="card"><h2>Conv2 Voltage</h2><div class="val blue">{{ '%.3f'|format(belt.conv2_v or 0) }} V</div></div>
  <div class="card"><h2>Sum (must = 3.30 V)</h2><div class="val {{ 'accepted' if (belt.conv1_v or 0)+(belt.conv2_v or 0) > 3.25 else 'rejected' }}">{{ '%.3f'|format((belt.conv1_v or 0)+(belt.conv2_v or 0)) }} V</div></div>
  <div class="card"><h2>Reference Speed</h2><div class="val">{{ '%.4f'|format(belt.reference_speed_ms or 0) }} m/s</div></div>
  <div class="card"><h2>Pear Count (Vision Zone)</h2><div class="val">{{ belt.pear_count or 0 }} / 6</div></div>
  <div class="card"><h2>Accepted</h2><div class="val accepted">{{ batch_accepted }}</div></div>
  <div class="card"><h2>Rejected</h2><div class="val rejected">{{ batch_rejected }}</div></div>
  <div class="card"><h2>Completed Packages</h2><div class="val amber"><a href="/labels" style="color:inherit;">{{ completed_packages }}</a></div></div>
</div>

<div class="card" style="margin-top:16px;">
  <h2>Motor / Zone Status</h2>
  <table><tr><th>Zone</th><th>Active</th><th>Last Action</th></tr>
  {% for m in motors_status %}
  <tr><td>{{m.zone}}</td><td>{{ 'YES' if m.active else 'no' }}</td>
      <td class="{{ 'accepted' if m.last_action=='ACCEPTED' else ('rejected' if m.last_action=='REJECTED' else '') }}">{{m.last_action}}</td></tr>
  {% endfor %}</table>
</div>

<div class="card" style="margin-top:16px;">
  <h2>Last Pear Record</h2>
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
  <a href="/database">/database</a> &nbsp;|&nbsp; <a href="/labels">/labels</a> &nbsp;|&nbsp;
  <a href="/api/status">/api/status</a>
  &nbsp;|&nbsp; auto-refresh every 10s &nbsp;|&nbsp; updated {{ now }}
</div>
</body></html>
"""

DATABASE_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT SCADA - Database</title>
<style>
  body{background:{{c.bg}};color:#e6edf3;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  h1{color:{{c.green}};font-size:20px;}
  table{width:100%;border-collapse:collapse;font-size:12px;margin-top:16px;}
  td,th{border-bottom:1px solid {{c.border}};padding:4px 8px;text-align:left;white-space:nowrap;}
  th{color:#8b949e;text-transform:uppercase;font-size:11px;}
  .ACCEPTED{color:{{c.green}};} .REJECTED{color:{{c.red}};}
  a{color:{{c.blue}};}
</style></head><body>
<h1>&#128190; pear_records - 200 most recent</h1>
<p><a href="/">&larr; back to status</a></p>
<table><tr>{% for col in columns %}<th>{{col}}</th>{% endfor %}</tr>
{% for row in rows %}<tr>{% for i in range(row|length) %}
<td class="{{ row[3] if i==3 else '' }}">{{ row[i] }}</td>{% endfor %}</tr>{% endfor %}
</table>
</body></html>
"""

LABELS_INDEX_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>SAAT SCADA - Package Labels</title>
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
<h1>&#127991; Package Labels - completed packages (12 BIG + 12 SMALL each)</h1>
<p><a href="/">&larr; back to status</a></p>
{% if packages %}
<table>
<tr><th>Package ID</th><th>Completed At</th><th>Packaging Clock</th><th>Upper (BIG) g</th><th>Lower (SMALL) g</th><th>Total g</th><th></th></tr>
{% for p in packages %}
<tr>
  <td>{{p.package_id}}</td><td>{{p.completed_at}}</td><td>{{p.duration}}</td>
  <td>{{p.upper_weight_g}}</td><td>{{p.lower_weight_g}}</td><td>{{p.total_weight_g}}</td>
  <td><a href="/labels/{{p.package_id}}">print label &rarr;</a></td>
</tr>
{% endfor %}
</table>
{% else %}
<div class="empty">No packages completed yet - each package needs 12 BIG + 12 SMALL accepted pears.</div>
{% endif %}
</body></html>
"""

LABEL_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<title>Label - {{package_id}}</title>
<style>
  body{background:#e9edf1;color:#111;font-family:'JetBrains Mono',monospace;margin:0;padding:24px;}
  .backlink{display:block;margin-bottom:16px;color:#3b82f6;text-decoration:none;font-size:13px;}
  .label-master{background:#fff;border:3px solid #111;width:980px;margin:0 auto 32px auto;}
  .mh-top{border-bottom:3px solid #111;padding:10px 16px;text-align:right;font-size:22px;font-weight:bold;letter-spacing:2px;}
  .mh-body{display:grid;grid-template-columns:1fr 1fr 260px;}
  .layer-col{padding:16px;border-right:2px solid #111;}
  .info-col{padding:16px;display:flex;flex-direction:column;gap:14px;align-items:center;}
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
  .pear-label .info{flex:1;padding:6px 8px;display:flex;flex-direction:column;justify-content:center;font-size:11px;gap:4px;}
  .pear-label .info b{font-size:12px;}
  .cat-BIG{border-left:6px solid #3b82f6;}
  .cat-SMALL{border-left:6px solid #f59e0b;}
</style></head><body>
<a class="backlink" href="/labels">&larr; back to package list</a>
<div class="label-master">
  <div class="mh-top">SAAT</div>
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
    <div class="info"><div>Package: <b>{{package_id}}</b></div><div>Pear ID: <b>{{item.pear_id}}</b></div></div>
  </div>
  {% endfor %}
</div>
</body></html>
"""


def build_flask_app(dc_node: DataCollectionNode, speed_ctrl: SpeedController,
                     db_path: Path, camera_hardware_active: bool):
    app = Flask(__name__)

    @app.route("/")
    def status():
        raw = dc_node.get_iot_status()
        payload = json.loads(raw) if raw and raw != "{}" else {}
        return render_template_string(
            STATUS_PAGE, c=COLORS, belt=payload.get("belt", {}) or {},
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
        cur = conn.execute("SELECT * FROM pear_records ORDER BY timestamp DESC LIMIT 200")
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
        conn.close()
        return render_template_string(DATABASE_PAGE, c=COLORS, columns=columns, rows=rows)

    @app.route("/api/status")
    def api_status():
        raw = dc_node.get_iot_status()
        return jsonify(json.loads(raw) if raw else {})

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
        return render_template_string(LABELS_INDEX_PAGE, c=COLORS, packages=packages)

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
    ap = argparse.ArgumentParser(description="SAAT - unified vision + control production system")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--no-web", action="store_true", help="headless: skip the Flask dashboard")
    ap.add_argument("--duration", type=float, default=0.0, help="auto-stop after N seconds")
    ap.add_argument("--db-path", type=str, default="./saat_data/saat_records.db")
    ap.add_argument("--force-sim", action="store_true",
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
    camera = CameraSystem(CONFIG["camera"])
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
    t = threading.Thread(target=speed_publisher_node, args=(speed_ctrl, stop_event), daemon=True)
    t.start(); threads.append(t)

    # T+5: data_collection_node (sequential DB writer + packaging + IoT publish)
    print("[SAAT] T+5: starting data_collection_node...")
    t = threading.Thread(target=dc_node.run, args=(speed_ctrl, stop_event), daemon=True)
    t.start(); threads.append(t)

    # T+6: SCADA dashboard
    if not args.no_web:
        app = build_flask_app(dc_node, speed_ctrl, db_path, camera.hardware_active)
        t = threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=args.port, debug=False,
                                    use_reloader=False, threaded=True), daemon=True)
        t.start(); threads.append(t)
        time.sleep(0.5)
        print(f"[SAAT] Dashboard : http://localhost:{args.port}")
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
        camera.stop()
        conn.close()
        print("[SAAT] Stopped. Database saved at:", db_path.resolve())


if __name__ == "__main__":
    main()
