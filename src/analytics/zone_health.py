from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..config import ComfortConfig
from ..store import sqlite_store
from .flow import compute_flow_tracking, FlowTrackingConfig


MERGE_TOLERANCE_SECONDS = 30


@dataclass
class ZoneHealthMetrics:
    # Identity / wiring
    station: str
    zone_root: str

    space_temp: Optional[str] = None
    space_temp_sp: Optional[str] = None
    flow: Optional[str] = None
    flow_sp: Optional[str] = None
    damper: Optional[str] = None
    reheat: Optional[str] = None
    fan_cmd: Optional[str] = None
    fan_status: Optional[str] = None

    # Comfort metrics
    comfort_samples: int = 0
    comfort_within_band_pct: Optional[float] = None
    comfort_mean_error_degF: Optional[float] = None

    # Flow metrics
    flow_samples: int = 0
    flow_within_band_pct: Optional[float] = None
    mean_flow_error_cfm: Optional[float] = None
    mean_flow_error_pct: Optional[float] = None

    # Damper sanity metrics
    damper_high_open_low_flow_pct: Optional[float] = None
    damper_closed_high_flow_pct: Optional[float] = None

    # Reheat metrics (mostly placeholder for now)
    reheat_waste_pct: Optional[float] = None

    # Fan metrics (Phase 2 will use these)
    fan_disagree_pct: Optional[float] = None
    fan_off_when_should_be_on_pct: Optional[float] = None
    fan_short_cycle_count: Optional[int] = None

    # Overall score (0–100, higher is healthier)
    overall_score: Optional[float] = None

    # NEW: diagnostic classification
    status: str = "no_data"          # "critical" | "warning" | "ok" | "no_data"
    reasons: List[str] = field(default_factory=list)


def _query_series_df(
    station: str,
    history_id: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
) -> pd.DataFrame:
    """Query a single history series from sqlite and return a DataFrame.

    Columns: timestamp (datetime, naive UTC), value (float).
    """
    if not history_id:
        return pd.DataFrame(columns=["timestamp", "value"])

    rows = sqlite_store.query_series(
        station=station,
        history_id=history_id,
        start=start,
        end=end,
    )
    if not rows:
        return pd.DataFrame(columns=["timestamp", "value"])

    df = pd.DataFrame(rows)
    # Expecting 'ts' and 'value' from sqlite_store
    if "ts" not in df.columns or "value" not in df.columns:
        return pd.DataFrame(columns=["timestamp", "value"])

    # Mixed ISO formats; parse robustly and normalize to naive UTC
    ts = pd.to_datetime(df["ts"], utc=True, format="mixed", errors="coerce")
    df = df.assign(timestamp=ts.dt.tz_convert("UTC").dt.tz_localize(None))
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values("timestamp")
    return df[["timestamp", "value"]].reset_index(drop=True)


def _parse_time(s: str) -> time:
    h, m = map(int, s.split(":"))
    return time(hour=h, minute=m)


def _compute_comfort_metrics(
    df_temp: pd.DataFrame,
    df_sp: pd.DataFrame,
    comfort_cfg: ComfortConfig,
) -> Tuple[int, Optional[float], Optional[float]]:
    """Return (samples, within_band_pct, mean_error_degF) for occupied window."""
    if df_temp.empty or df_sp.empty:
        return 0, None, None

    # Align temp and setpoint by nearest timestamp
    merged = pd.merge_asof(
        df_temp.sort_values("timestamp"),
        df_sp.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=MERGE_TOLERANCE_SECONDS),
        suffixes=("_temp", "_sp"),
    )

    merged = merged.dropna(subset=["value_temp", "value_sp"])
    if merged.empty:
        return 0, None, None

    occ_start = _parse_time(comfort_cfg.occupied_start)
    occ_end = _parse_time(comfort_cfg.occupied_end)
    merged["time"] = merged["timestamp"].dt.time

    occupied = merged[
        (merged["time"] >= occ_start) & (merged["time"] <= occ_end)
    ].copy()

    if occupied.empty:
        return 0, None, None

    occupied["error"] = occupied["value_temp"] - occupied["value_sp"]
    occupied["abs_error"] = occupied["error"].abs()

    samples = int(len(occupied))
    within_band = (occupied["abs_error"] <= comfort_cfg.comfort_band_degF).sum()
    within_band_pct = float(within_band / samples * 100.0)
    mean_error = float(occupied["error"].mean())

    return samples, within_band_pct, mean_error


def _compute_flow_and_damper_metrics(
    df_flow: pd.DataFrame,
    df_flow_sp: pd.DataFrame,
    df_damper: pd.DataFrame,
) -> Tuple[int, Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Compute flow tracking and damper sanity metrics.

    Returns:
        flow_samples,
        flow_within_band_pct,
        mean_error_cfm,
        damper_high_open_low_flow_pct,
        damper_closed_high_flow_pct
    """
    flow_samples = 0
    flow_within_band_pct = None
    mean_error_cfm = None
    damper_high_open_low_flow_pct = None
    damper_closed_high_flow_pct = None

    # Flow tracking
    if not df_flow.empty:
        if df_flow_sp.empty:
            # Treat single series as flow with no SP; we can still count samples but not tracking
            flow_samples = int(len(df_flow))
        else:
            # Adapt frames for compute_flow_tracking
            cfg = FlowTrackingConfig()
            cfg.timestamp_column = "timestamp"
            cfg.value_column = "value"
            cfg.merge_tolerance_seconds = MERGE_TOLERANCE_SECONDS

            metrics = compute_flow_tracking(df_flow, df_flow_sp, cfg)
            flow_samples = int(metrics.get("samples", 0))
            flow_within_band_pct = metrics.get("within_band_pct")
            mean_error_cfm = metrics.get("mean_error_cfm")

    # Damper sanity (requires at least flow + damper; use flow_sp if available)
    if df_damper.empty or df_flow.empty:
        return (
            flow_samples,
            flow_within_band_pct,
            mean_error_cfm,
            damper_high_open_low_flow_pct,
            damper_closed_high_flow_pct,
        )

    merged = pd.merge_asof(
        df_damper.sort_values("timestamp").rename(columns={"value": "damper"}),
        df_flow.sort_values("timestamp").rename(columns={"value": "flow"}),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=MERGE_TOLERANCE_SECONDS),
    )

    if not df_flow_sp.empty:
        merged = pd.merge_asof(
            merged.sort_values("timestamp"),
            df_flow_sp.sort_values("timestamp").rename(columns={"value": "flow_sp"}),
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta(seconds=MERGE_TOLERANCE_SECONDS),
        )
    else:
        merged["flow_sp"] = None

    merged = merged.dropna(subset=["damper", "flow"])
    if merged.empty:
        return (
            flow_samples,
            flow_within_band_pct,
            mean_error_cfm,
            damper_high_open_low_flow_pct,
            damper_closed_high_flow_pct,
        )

    total = len(merged)

    # Define "high open" and "closed"
    high_open = merged["damper"] >= 80.0
    closed = merged["damper"] <= 5.0

    # Define "low flow" vs "high flow" relative to SP if present; otherwise heuristics
    if merged["flow_sp"].notna().any():
        # Use SP if we have it
        merged_valid_sp = merged.dropna(subset=["flow_sp"]).copy()
        if not merged_valid_sp.empty:
            low_flow = merged_valid_sp["flow"] < 0.5 * merged_valid_sp["flow_sp"]
            high_flow = merged_valid_sp["flow"] > 0.8 * merged_valid_sp["flow_sp"]
            high_open_low_flow = (high_open.loc[merged_valid_sp.index] & low_flow).sum()
            closed_high_flow = (closed.loc[merged_valid_sp.index] & high_flow).sum()
            denom = len(merged_valid_sp)
        else:
            high_open_low_flow = 0
            closed_high_flow = 0
            denom = total
    else:
        # No SP – use relative thresholds based on observed flow distribution
        f = merged["flow"]
        if f.max() <= 0:
            high_open_low_flow = 0
            closed_high_flow = 0
            denom = total
        else:
            low_flow = f < 0.3 * f.max()
            high_flow = f > 0.7 * f.max()
            high_open_low_flow = (high_open & low_flow).sum()
            closed_high_flow = (closed & high_flow).sum()
            denom = total

    if denom > 0:
        damper_high_open_low_flow_pct = float(high_open_low_flow / denom * 100.0)
        damper_closed_high_flow_pct = float(closed_high_flow / denom * 100.0)

    return (
        flow_samples,
        flow_within_band_pct,
        mean_error_cfm,
        damper_high_open_low_flow_pct,
        damper_closed_high_flow_pct,
    )


def _compute_reheat_waste_metrics(
    df_reheat: pd.DataFrame,
    df_temp: pd.DataFrame,
    df_sp: pd.DataFrame,
    comfort_cfg: ComfortConfig,
) -> Optional[float]:
    """Estimate reheat waste percentage (occupied samples with reheat > 0 while hot).

    For now we keep this simple and do not feed it into status; Phase 3 will refine.
    """
    if df_reheat.empty or df_temp.empty or df_sp.empty:
        return None

    merged = pd.merge_asof(
        df_reheat.sort_values("timestamp").rename(columns={"value": "reheat"}),
        df_temp.sort_values("timestamp").rename(columns={"value": "temp"}),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=MERGE_TOLERANCE_SECONDS),
    )

    merged = pd.merge_asof(
        merged.sort_values("timestamp"),
        df_sp.sort_values("timestamp").rename(columns={"value": "sp"}),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=MERGE_TOLERANCE_SECONDS),
    )

    merged = merged.dropna(subset=["reheat", "temp", "sp"])
    if merged.empty:
        return None

    occ_start = _parse_time(comfort_cfg.occupied_start)
    occ_end = _parse_time(comfort_cfg.occupied_end)
    merged["time"] = merged["timestamp"].dt.time

    occupied = merged[
        (merged["time"] >= occ_start) & (merged["time"] <= occ_end)
    ].copy()
    if occupied.empty:
        return None

    # Simple heuristic: any positive reheat above 10% while > 1°F above setpoint
    hot_and_reheat = (occupied["reheat"] > 10.0) & (
        occupied["temp"] >= occupied["sp"] + 1.0
    )
    total = len(occupied)
    waste_pct = float(hot_and_reheat.sum() / total * 100.0) if total > 0 else None
    return waste_pct


def _compute_overall_score(metrics: ZoneHealthMetrics) -> Optional[float]:
    """Combine metrics into a single 0–100 score.

    Weights (initial):
      - comfort: ×3
      - flow: ×2
      - damper: ×1 (penalize anomalies)
      - reheat: ×1 (penalize waste when present)
    """
    components: List[float] = []
    weights: List[float] = []

    # Comfort (direct)
    if metrics.comfort_within_band_pct is not None:
        components.append(metrics.comfort_within_band_pct)
        weights.append(3.0)

    # Flow (direct)
    if metrics.flow_within_band_pct is not None:
        components.append(metrics.flow_within_band_pct)
        weights.append(2.0)

    # Damper anomalies (inverse)
    if metrics.damper_high_open_low_flow_pct is not None:
        components.append(max(0.0, 100.0 - metrics.damper_high_open_low_flow_pct))
        weights.append(1.0)
    if metrics.damper_closed_high_flow_pct is not None:
        components.append(max(0.0, 100.0 - metrics.damper_closed_high_flow_pct))
        weights.append(1.0)

    # Reheat waste (inverse)
    if metrics.reheat_waste_pct is not None:
        components.append(max(0.0, 100.0 - metrics.reheat_waste_pct))
        weights.append(1.0)

    if not components:
        return None

    weighted = sum(c * w for c, w in zip(components, weights))
    total_w = sum(weights)
    return float(weighted / total_w)


def _derive_status_and_reasons(m: ZoneHealthMetrics) -> None:
    """Set m.status and m.reasons based on current metrics.

    Rules (initial):
      - no_data:
          - comfort_samples == 0 OR both comfort_within_band_pct and flow_within_band_pct are None
      - critical:
          - comfort_within_band_pct < 50 and mean_error <= -3.0  -> cold_zone
          - comfort_within_band_pct < 50 and mean_error >= +3.0  -> hot_zone
          - flow_within_band_pct < 40                            -> poor_flow_tracking
          - damper_high_open_low_flow_pct > 30                   -> damper_leak
          - damper_closed_high_flow_pct > 30                     -> damper_stuck
      - warning:
          - comfort_within_band_pct between 50–80
          - flow_within_band_pct between 40–70
          - damper anomaly pct between 10–30
      - ok:
          - everything else
    """
    reasons: List[str] = []

    # Determine if we effectively have no data
    if m.comfort_samples == 0 and (
        m.flow_within_band_pct is None and m.comfort_within_band_pct is None
    ):
        m.status = "no_data"
        m.reasons = []
        return

    # ------------------
    # Critical conditions
    # ------------------
    critical = False

    if m.comfort_within_band_pct is not None and m.comfort_mean_error_degF is not None:
        if m.comfort_within_band_pct < 50.0 and m.comfort_mean_error_degF <= -3.0:
            critical = True
            reasons.append("cold_zone")
        if m.comfort_within_band_pct < 50.0 and m.comfort_mean_error_degF >= 3.0:
            critical = True
            reasons.append("hot_zone")

    if m.flow_within_band_pct is not None and m.flow_within_band_pct < 40.0:
        critical = True
        reasons.append("poor_flow_tracking")

    if (
        m.damper_high_open_low_flow_pct is not None
        and m.damper_high_open_low_flow_pct > 30.0
    ):
        critical = True
        reasons.append("damper_leak")

    if (
        m.damper_closed_high_flow_pct is not None
        and m.damper_closed_high_flow_pct > 30.0
    ):
        critical = True
        reasons.append("damper_stuck")

    if critical:
        m.status = "critical"
        m.reasons = sorted(set(reasons))
        return

    # ---------------
    # Warning signals
    # ---------------
    warning = False

    if m.comfort_within_band_pct is not None:
        if 50.0 <= m.comfort_within_band_pct < 80.0:
            warning = True
            if m.comfort_mean_error_degF is not None:
                if m.comfort_mean_error_degF <= -2.0:
                    reasons.append("slightly_cold_zone")
                elif m.comfort_mean_error_degF >= 2.0:
                    reasons.append("slightly_hot_zone")
                else:
                    reasons.append("borderline_comfort")
            else:
                reasons.append("borderline_comfort")

    if m.flow_within_band_pct is not None:
        if 40.0 <= m.flow_within_band_pct < 70.0:
            warning = True
            reasons.append("borderline_flow_tracking")

    if m.damper_high_open_low_flow_pct is not None:
        if 10.0 < m.damper_high_open_low_flow_pct <= 30.0:
            warning = True
            reasons.append("possible_damper_leak")

    if m.damper_closed_high_flow_pct is not None:
        if 10.0 < m.damper_closed_high_flow_pct <= 30.0:
            warning = True
            reasons.append("possible_damper_stuck")

    if warning:
        m.status = "warning"
        m.reasons = sorted(set(reasons))
        return

    # ----
    # OK
    # ----
    m.status = "ok"
    m.reasons = []


def compute_zone_health(
    station: str,
    zone_root: str,
    zone_info: Dict[str, Any],
    comfort_cfg: ComfortConfig,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> ZoneHealthMetrics:
    """Compute ZoneHealthMetrics for a single zone root.

    zone_info is typically a dict from zone_pairs_as_dicts()[station][zone_root].
    """
    metrics = ZoneHealthMetrics(
        station=station,
        zone_root=zone_root,
        space_temp=zone_info.get("space_temp"),
        space_temp_sp=zone_info.get("space_temp_sp"),
        flow=zone_info.get("flow"),
        flow_sp=zone_info.get("flow_sp"),
        damper=zone_info.get("damper"),
        reheat=zone_info.get("reheat"),
        fan_cmd=zone_info.get("fan_cmd"),
        fan_status=zone_info.get("fan_status"),
    )

    # Default time range: last 24 hours if not provided
    if end is None:
        end = datetime.utcnow()
    if start is None:
        start = end - timedelta(hours=24)

    # Query all series we might use
    df_temp = _query_series_df(station, metrics.space_temp, start, end)
    df_sp = _query_series_df(station, metrics.space_temp_sp, start, end)
    df_flow = _query_series_df(station, metrics.flow, start, end)
    df_flow_sp = _query_series_df(station, metrics.flow_sp, start, end)
    df_damper = _query_series_df(station, metrics.damper, start, end)
    df_reheat = _query_series_df(station, metrics.reheat, start, end)

    # Comfort
    (
        metrics.comfort_samples,
        metrics.comfort_within_band_pct,
        metrics.comfort_mean_error_degF,
    ) = _compute_comfort_metrics(df_temp, df_sp, comfort_cfg)

    # Flow + damper
    (
        metrics.flow_samples,
        metrics.flow_within_band_pct,
        metrics.mean_flow_error_cfm,
        metrics.damper_high_open_low_flow_pct,
        metrics.damper_closed_high_flow_pct,
    ) = _compute_flow_and_damper_metrics(df_flow, df_flow_sp, df_damper)

    # Reheat waste (not yet used in status)
    metrics.reheat_waste_pct = _compute_reheat_waste_metrics(
        df_reheat, df_temp, df_sp, comfort_cfg
    )

    # Overall score
    metrics.overall_score = _compute_overall_score(metrics)

    # Status + reasons
    _derive_status_and_reasons(metrics)

    return metrics


def zone_health_to_dict(metrics: ZoneHealthMetrics) -> Dict[str, Any]:
    """Flatten ZoneHealthMetrics into a JSON-serializable dict."""
    d = asdict(metrics)
    # Ensure reasons is always a list, status always a string
    d.setdefault("status", "no_data")
    d.setdefault("reasons", [])
    return d
