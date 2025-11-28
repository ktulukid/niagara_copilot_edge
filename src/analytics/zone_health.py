# src/analytics/zone_health.py

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import pandas as pd

from ..config import ComfortConfig
from ..analytics.comfort import compute_zone_comfort
from ..analytics.flow import compute_flow_tracking, FlowTrackingConfig
from ..store import sqlite_store


@dataclass
class ZoneHealthMetrics:
    zone_root: str

    # History IDs for context
    space_temp: Optional[str]
    space_temp_sp: Optional[str]
    flow: Optional[str]
    flow_sp: Optional[str]
    damper: Optional[str]
    reheat: Optional[str]
    fan_cmd: Optional[str]
    fan_status: Optional[str]

    # Comfort
    comfort_samples: int = 0
    comfort_within_band_pct: Optional[float] = None
    comfort_mean_error_degF: Optional[float] = None

    # Flow tracking
    flow_samples: int = 0
    flow_within_band_pct: Optional[float] = None

    # Flow vs damper sanity checks
    damper_high_open_low_flow_pct: Optional[float] = None
    damper_closed_high_flow_pct: Optional[float] = None

    # Reheat heat-wasting detection
    reheat_waste_pct: Optional[float] = None

    # Aggregate score (0–100, higher is better)
    overall_score: Optional[float] = None


def _query_series_df(
    station: str,
    history_id: Optional[str],
    start: datetime,
    end: datetime,
    value_col: str = "value",
) -> pd.DataFrame:
    """
    Load a single history series from SQLite into a DataFrame with:
      - timestamp: naive UTC datetime
      - <value_col>: float or numeric

    Handles mixed ISO8601 formats (with/without microseconds, with tz offset).
    """
    if not history_id:
        return pd.DataFrame(columns=["timestamp", value_col])

    rows = sqlite_store.query_series(
        station=station,
        history_id=history_id,
        start=start,
        end=end,
        limit=10_000,
    )
    if not rows:
        return pd.DataFrame(columns=["timestamp", value_col])

    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        # Parse mixed ISO8601 strings and normalise to naive UTC
        ts = pd.to_datetime(
            df["timestamp"],
            format="mixed",   # allows both with and without microseconds
            utc=True,
            errors="coerce",
        )
        # Drop timezone (keep UTC clock time as naive)
        df["timestamp"] = ts.dt.tz_localize(None)

    df = df.rename(columns={"timestamp": "timestamp", "value": value_col})
    df = df.sort_values("timestamp")
    return df



def _compute_comfort(
    station: str,
    zone_root: str,
    space_temp_id: Optional[str],
    space_temp_sp_id: Optional[str],
    start: datetime,
    end: datetime,
    comfort_cfg: ComfortConfig,
) -> Dict[str, Any]:
    if not space_temp_id or not space_temp_sp_id:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    df_t = _query_series_df(station, space_temp_id, start, end, value_col=comfort_cfg.temp_column)
    df_sp = _query_series_df(station, space_temp_sp_id, start, end, value_col=comfort_cfg.setpoint_column)
    if df_t.empty or df_sp.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    ts_col = comfort_cfg.timestamp_column
    df_t = df_t.rename(columns={"timestamp": ts_col})
    df_sp = df_sp.rename(columns={"timestamp": ts_col})

    df_t = df_t.sort_values(ts_col)
    df_sp = df_sp.sort_values(ts_col)

    merged = pd.merge_asof(
        df_t,
        df_sp[[ts_col, comfort_cfg.setpoint_column]],
        on=ts_col,
        direction="nearest",
        tolerance=pd.Timedelta(seconds=30),
    )
    merged = merged.dropna(subset=[comfort_cfg.setpoint_column])

    if merged.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    metrics = compute_zone_comfort(merged, comfort_cfg)
    return metrics


def _compute_flow_and_damper(
    station: str,
    flow_id: Optional[str],
    flow_sp_id: Optional[str],
    damper_id: Optional[str],
    start: datetime,
    end: datetime,
) -> Dict[str, Any]:
    # Flow tracking using existing analytics
    df_flow = _query_series_df(station, flow_id, start, end, value_col="value")
    df_sp = _query_series_df(station, flow_sp_id, start, end, value_col="value") if flow_sp_id else None

    flow_metrics = compute_flow_tracking(df_flow, df_sp, cfg=FlowTrackingConfig())

    # Flow vs damper sanity only if we have both series
    if not flow_id or not damper_id:
        return {
            "flow_samples": flow_metrics.get("samples", 0),
            "flow_within_band_pct": flow_metrics.get("within_band_pct"),
            "damper_high_open_low_flow_pct": None,
            "damper_closed_high_flow_pct": None,
        }

    df_damper = _query_series_df(station, damper_id, start, end, value_col="damper")
    if df_damper.empty or df_flow.empty:
        return {
            "flow_samples": flow_metrics.get("samples", 0),
            "flow_within_band_pct": flow_metrics.get("within_band_pct"),
            "damper_high_open_low_flow_pct": None,
            "damper_closed_high_flow_pct": None,
        }

    # Align flow and damper
    merged = pd.merge_asof(
        df_flow.sort_values("timestamp").rename(columns={"value": "flow"}),
        df_damper.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=30),
    ).dropna(subset=["flow", "damper"])

    if merged.empty:
        return {
            "flow_samples": flow_metrics.get("samples", 0),
            "flow_within_band_pct": flow_metrics.get("within_band_pct"),
            "damper_high_open_low_flow_pct": None,
            "damper_closed_high_flow_pct": None,
        }

    # Thresholds based on median flow
    median_flow = merged["flow"].median()
    if median_flow <= 0:
        low_flow_thr = None
        high_flow_thr = None
    else:
        low_flow_thr = 0.3 * median_flow
        high_flow_thr = 0.7 * median_flow

    if low_flow_thr is None or high_flow_thr is None:
        high_open_low_flow_pct = None
        closed_high_flow_pct = None
    else:
        high_open_low_flow = (merged["damper"] >= 90.0) & (merged["flow"] < low_flow_thr)
        closed_high_flow = (merged["damper"] <= 10.0) & (merged["flow"] > high_flow_thr)

        total = len(merged)
        high_open_low_flow_pct = float(high_open_low_flow.sum() * 100.0 / total) if total > 0 else None
        closed_high_flow_pct = float(closed_high_flow.sum() * 100.0 / total) if total > 0 else None

    return {
        "flow_samples": flow_metrics.get("samples", 0),
        "flow_within_band_pct": flow_metrics.get("within_band_pct"),
        "damper_high_open_low_flow_pct": high_open_low_flow_pct,
        "damper_closed_high_flow_pct": closed_high_flow_pct,
    }


def _compute_reheat_waste(
    station: str,
    reheat_id: Optional[str],
    space_temp_id: Optional[str],
    space_temp_sp_id: Optional[str],
    start: datetime,
    end: datetime,
    comfort_cfg: ComfortConfig,
    waste_deadband_degF: float = 1.0,
) -> Optional[float]:
    if not reheat_id or not space_temp_id or not space_temp_sp_id:
        return None

    df_t = _query_series_df(station, space_temp_id, start, end, value_col=comfort_cfg.temp_column)
    df_sp = _query_series_df(station, space_temp_sp_id, start, end, value_col=comfort_cfg.setpoint_column)
    df_rh = _query_series_df(station, reheat_id, start, end, value_col="reheat")

    if df_t.empty or df_sp.empty or df_rh.empty:
        return None

    ts_col = comfort_cfg.timestamp_column
    df_t = df_t.rename(columns={"timestamp": ts_col})
    df_sp = df_sp.rename(columns={"timestamp": ts_col})
    df_rh = df_rh.rename(columns={"timestamp": ts_col})

    df_t = df_t.sort_values(ts_col)
    df_sp = df_sp.sort_values(ts_col)
    df_rh = df_rh.sort_values(ts_col)

    merged = pd.merge_asof(
        df_t,
        df_sp[[ts_col, comfort_cfg.setpoint_column]],
        on=ts_col,
        direction="nearest",
        tolerance=pd.Timedelta(seconds=30),
    )
    merged = pd.merge_asof(
        merged,
        df_rh[[ts_col, "reheat"]],
        on=ts_col,
        direction="nearest",
        tolerance=pd.Timedelta(seconds=30),
    )

    merged = merged.dropna(subset=[comfort_cfg.setpoint_column, "reheat"])
    if merged.empty:
        return None

    # Reheat "waste" where reheat > 0 but space temp is already above SP + deadband
    temp_col = comfort_cfg.temp_column
    sp_col = comfort_cfg.setpoint_column

    reheat_on = merged["reheat"] > 0.0
    waste_mask = reheat_on & (merged[temp_col] > merged[sp_col] + waste_deadband_degF)

    total_on = int(reheat_on.sum())
    if total_on == 0:
        return None

    waste_pct = float(waste_mask.sum() * 100.0 / total_on)
    return waste_pct


def _compute_overall_score(
    comfort_within_band_pct: Optional[float],
    flow_within_band_pct: Optional[float],
    damper_high_open_low_flow_pct: Optional[float],
    damper_closed_high_flow_pct: Optional[float],
    reheat_waste_pct: Optional[float],
) -> Optional[float]:
    # Higher comfort/flow percentages are good; higher "bad" percentages are bad.
    parts = []
    weights = []

    # Comfort: weight 3
    if comfort_within_band_pct is not None:
        parts.append(comfort_within_band_pct)
        weights.append(3.0)

    # Flow tracking: weight 2
    if flow_within_band_pct is not None:
        parts.append(flow_within_band_pct)
        weights.append(2.0)

    # Damper issues: we interpret as 100 - bad_percent
    if damper_high_open_low_flow_pct is not None:
        parts.append(max(0.0, 100.0 - damper_high_open_low_flow_pct))
        weights.append(1.0)
    if damper_closed_high_flow_pct is not None:
        parts.append(max(0.0, 100.0 - damper_closed_high_flow_pct))
        weights.append(1.0)

    # Reheat waste: interpret as 100 - waste
    if reheat_waste_pct is not None:
        parts.append(max(0.0, 100.0 - reheat_waste_pct))
        weights.append(1.0)

    if not weights:
        return None

    score = sum(p * w for p, w in zip(parts, weights)) / sum(weights)
    return float(score)


def compute_zone_health(
    station: str,
    zone_root: str,
    zone_info: Dict[str, Any],
    comfort_cfg: ComfortConfig,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> ZoneHealthMetrics:
    """
    Compute comfort, flow tracking, damper sanity, reheat waste, and an
    overall score for a single zone/equipment root.
    """
    if end is None:
        end = datetime.utcnow()
    if start is None:
        start = end - timedelta(hours=24)

    space_temp_id = zone_info.get("space_temp")
    space_temp_sp_id = zone_info.get("space_temp_sp")
    flow_id = zone_info.get("flow")
    flow_sp_id = zone_info.get("flow_sp")
    damper_id = zone_info.get("damper")
    reheat_id = zone_info.get("reheat")
    fan_cmd_id = zone_info.get("fan_cmd")
    fan_status_id = zone_info.get("fan_status")

    # Comfort
    comfort = _compute_comfort(
        station=station,
        zone_root=zone_root,
        space_temp_id=space_temp_id,
        space_temp_sp_id=space_temp_sp_id,
        start=start,
        end=end,
        comfort_cfg=comfort_cfg,
    )

    # Flow + damper
    flow_damper = _compute_flow_and_damper(
        station=station,
        flow_id=flow_id,
        flow_sp_id=flow_sp_id,
        damper_id=damper_id,
        start=start,
        end=end,
    )

    # Reheat waste
    reheat_waste = _compute_reheat_waste(
        station=station,
        reheat_id=reheat_id,
        space_temp_id=space_temp_id,
        space_temp_sp_id=space_temp_sp_id,
        start=start,
        end=end,
        comfort_cfg=comfort_cfg,
    )

    comfort_samples = int(comfort.get("samples", 0) or 0)
    comfort_within = comfort.get("within_band_pct")
    comfort_err = comfort.get("mean_error_degF")

    flow_samples = int(flow_damper.get("flow_samples", 0) or 0)
    flow_within = flow_damper.get("flow_within_band_pct")
    damper_high_open_low_flow_pct = flow_damper.get("damper_high_open_low_flow_pct")
    damper_closed_high_flow_pct = flow_damper.get("damper_closed_high_flow_pct")

    overall = _compute_overall_score(
        comfort_within_band_pct=comfort_within,
        flow_within_band_pct=flow_within,
        damper_high_open_low_flow_pct=damper_high_open_low_flow_pct,
        damper_closed_high_flow_pct=damper_closed_high_flow_pct,
        reheat_waste_pct=reheat_waste,
    )

    return ZoneHealthMetrics(
        zone_root=zone_root,
        space_temp=space_temp_id,
        space_temp_sp=space_temp_sp_id,
        flow=flow_id,
        flow_sp=flow_sp_id,
        damper=damper_id,
        reheat=reheat_id,
        fan_cmd=fan_cmd_id,
        fan_status=fan_status_id,
        comfort_samples=comfort_samples,
        comfort_within_band_pct=comfort_within,
        comfort_mean_error_degF=comfort_err,
        flow_samples=flow_samples,
        flow_within_band_pct=flow_within,
        damper_high_open_low_flow_pct=damper_high_open_low_flow_pct,
        damper_closed_high_flow_pct=damper_closed_high_flow_pct,
        reheat_waste_pct=reheat_waste,
        overall_score=overall,
    )


def zone_health_to_dict(m: ZoneHealthMetrics) -> Dict[str, Any]:
    return asdict(m)
