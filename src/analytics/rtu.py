# src/analytics/rtu.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

# Reuse the same sqlite → DataFrame helper as zone_health
from .zone_health import _query_series_df


# -----------------------------
# Dataclasses for RTU metrics
# -----------------------------


@dataclass
class FanMetrics:
    samples: int = 0
    on_pct: Optional[float] = None
    short_cycle_count: Optional[int] = None


@dataclass
class CoolingMetrics:
    samples: int = 0
    short_cycle_count: Optional[int] = None


@dataclass
class DischargeAirMetrics:
    samples: int = 0
    within_band_pct: Optional[float] = None
    mean_error_degF: Optional[float] = None


@dataclass
class RTUHealthMetrics:
    station: str
    zone_root: str

    equipment: Optional[str] = None

    fan_metrics: FanMetrics = field(default_factory=FanMetrics)
    cooling_metrics: CoolingMetrics = field(default_factory=CoolingMetrics)
    discharge_metrics: DischargeAirMetrics = field(default_factory=DischargeAirMetrics)

    status: str = "no_data"  # "critical" | "warning" | "ok" | "no_data"
    reasons: List[str] = field(default_factory=list)


# -----------------------------
# Low-level helpers
# -----------------------------


def _compute_binary_cycles(
    df: pd.DataFrame,
    threshold: float = 0.5,
    min_cycle_minutes: float = 10.0,
) -> Dict[str, Any]:
    """
    Given a DataFrame with columns [timestamp, value] where value is numeric,
    treat value > threshold as "ON" and compute:
      - samples
      - on_pct
      - short_cycle_count (ON periods shorter than min_cycle_minutes)
    """
    if df.empty:
        return {
            "samples": 0,
            "on_pct": None,
            "short_cycle_count": None,
        }

    df = df.sort_values("timestamp").copy()
    on = df["value"] > threshold
    ts = df["timestamp"]

    samples = int(len(df))
    on_pct = float(on.mean() * 100.0)

    short_cycles = 0
    prev_state = bool(on.iloc[0])
    on_start: Optional[datetime] = ts.iloc[0] if prev_state else None

    for t, state in zip(ts.iloc[1:], on.iloc[1:]):
        state = bool(state)
        if state == prev_state:
            continue

        # Transition
        if prev_state and not state:
            # ON -> OFF: complete a cycle
            if on_start is not None:
                dur_min = (t - on_start).total_seconds() / 60.0
                if dur_min < min_cycle_minutes:
                    short_cycles += 1
            on_start = None
        elif not prev_state and state:
            # OFF -> ON: new ON period
            on_start = t

        prev_state = state

    return {
        "samples": samples,
        "on_pct": on_pct,
        "short_cycle_count": short_cycles,
    }


def _compute_discharge_metrics(
    df_da: pd.DataFrame,
    df_da_sp: pd.DataFrame,
    band_degF: float = 2.0,
) -> Dict[str, Any]:
    """
    Basic discharge-air tracking:
      - align DA and DA setpoint by nearest timestamp
      - compute error = DA - SP
      - within_band_pct for |error| <= band_degF over all merged samples
    """
    if df_da.empty or df_da_sp.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    df_da = df_da.sort_values("timestamp").rename(columns={"value": "da"})
    df_sp = df_da_sp.sort_values("timestamp").rename(columns={"value": "da_sp"})

    merged = pd.merge_asof(
        df_da,
        df_sp,
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=60),
    )

    merged = merged.dropna(subset=["da", "da_sp"])
    if merged.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    merged["error"] = merged["da"] - merged["da_sp"]
    merged["abs_error"] = merged["error"].abs()

    samples = int(len(merged))
    within_band = int((merged["abs_error"] <= band_degF).sum())
    within_band_pct = float(within_band / samples * 100.0)
    mean_error = float(merged["error"].mean())

    return {
        "samples": samples,
        "within_band_pct": within_band_pct,
        "mean_error_degF": mean_error,
    }


# -----------------------------
# Public API
# -----------------------------


def compute_rtu_health(
    station: str,
    zone_root: str,
    zone_info: Dict[str, Any],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> RTUHealthMetrics:
    """
    Compute RTUHealthMetrics for an equipment-level zone_root.

    We lean on zone_pairs to give us roles like:
      - fan_cmd / fan_status
      - compressor_cmd / compressor_status
      - discharge_air / discharge_air_sp  (if you add these roles later)

    At this stage we implement:
      - Fan ON% and short-cycling
      - Cooling command short-cycling
      - Discharge-air tracking vs setpoint
    """
    m = RTUHealthMetrics(
        station=station,
        zone_root=zone_root,
        equipment=zone_info.get("equipment"),
    )

    if end is None:
        end = datetime.utcnow()
    if start is None:
        start = end - pd.Timedelta(hours=24)

    # --- Fan metrics ---------------------------------------------------------
    fan_cmd_id = zone_info.get("fan_cmd")
    fan_status_id = zone_info.get("fan_status")

    df_cmd = _query_series_df(station, fan_cmd_id, start, end)
    df_status = _query_series_df(station, fan_status_id, start, end)

    # Prefer status if we have it; fall back to command
    df_fan = df_status if not df_status.empty else df_cmd

    fan_raw = _compute_binary_cycles(df_fan)
    m.fan_metrics = FanMetrics(
        samples=fan_raw["samples"],
        on_pct=fan_raw["on_pct"],
        short_cycle_count=fan_raw["short_cycle_count"],
    )

    # --- Cooling metrics (compressor command) -------------------------------
    compressor_cmd_id = zone_info.get("compressor_cmd")
    cooling_cmd_id = zone_info.get("cooling_valve")  # optional future role

    df_comp = _query_series_df(station, compressor_cmd_id, start, end)
    df_cool = _query_series_df(station, cooling_cmd_id, start, end)

    df_cooling = df_comp if not df_comp.empty else df_cool
    cool_raw = _compute_binary_cycles(df_cooling)

    m.cooling_metrics = CoolingMetrics(
        samples=cool_raw["samples"],
        short_cycle_count=cool_raw["short_cycle_count"],
    )

    # --- Discharge-air tracking (if you add those roles later) --------------
    discharge_id = zone_info.get("discharge_air")
    discharge_sp_id = zone_info.get("discharge_air_sp")

    df_da = _query_series_df(station, discharge_id, start, end)
    df_da_sp = _query_series_df(station, discharge_sp_id, start, end)

    da_raw = _compute_discharge_metrics(df_da, df_da_sp)
    m.discharge_metrics = DischargeAirMetrics(
        samples=da_raw["samples"],
        within_band_pct=da_raw["within_band_pct"],
        mean_error_degF=da_raw["mean_error_degF"],
    )

    # --- Status + reasons ----------------------------------------------------
    reasons: List[str] = []
    critical = False
    warning = False

    # Fan short-cycling heuristic
    if m.fan_metrics.short_cycle_count is not None:
        if m.fan_metrics.short_cycle_count >= 10:
            critical = True
            reasons.append("fan_short_cycling")
        elif m.fan_metrics.short_cycle_count >= 3:
            warning = True
            reasons.append("possible_fan_short_cycling")

    # Cooling short-cycling heuristic
    if m.cooling_metrics.short_cycle_count is not None:
        if m.cooling_metrics.short_cycle_count >= 10:
            critical = True
            reasons.append("cooling_short_cycling")
        elif m.cooling_metrics.short_cycle_count >= 3:
            warning = True
            reasons.append("possible_cooling_short_cycling")

    # Discharge-air tracking heuristic
    if m.discharge_metrics.within_band_pct is not None:
        if m.discharge_metrics.within_band_pct < 50.0:
            warning = True
            reasons.append("discharge_air_not_meeting_sp")
        elif m.discharge_metrics.within_band_pct < 70.0:
            warning = True
            reasons.append("borderline_discharge_air_tracking")

    if critical:
        m.status = "critical"
    elif warning:
        m.status = "warning"
    elif (
        m.fan_metrics.samples > 0
        or m.cooling_metrics.samples > 0
        or m.discharge_metrics.samples > 0
    ):
        m.status = "ok"
    else:
        m.status = "no_data"

    m.reasons = sorted(set(reasons))
    return m


def rtu_health_to_dict(m: RTUHealthMetrics) -> Dict[str, Any]:
    """
    Flatten RTUHealthMetrics into a JSON-serializable dict for API responses.
    """
    return {
        "station": m.station,
        "zone_root": m.zone_root,
        "equipment": m.equipment,
        "fan_metrics": {
            "samples": m.fan_metrics.samples,
            "on_pct": m.fan_metrics.on_pct,
            "short_cycle_count": m.fan_metrics.short_cycle_count,
        },
        "cooling_metrics": {
            "samples": m.cooling_metrics.samples,
            "short_cycle_count": m.cooling_metrics.short_cycle_count,
        },
        "discharge_metrics": {
            "samples": m.discharge_metrics.samples,
            "within_band_pct": m.discharge_metrics.within_band_pct,
            "mean_error_degF": m.discharge_metrics.mean_error_degF,
        },
        "status": m.status,
        "reasons": m.reasons,
    }
