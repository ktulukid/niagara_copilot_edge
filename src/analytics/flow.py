# src/analytics/flow.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class FlowTrackingConfig:
    timestamp_column: str = "timestamp"
    value_column: str = "value"
    # Percentage tolerance (e.g. 0.1 = ±10% of setpoint)
    pct_tolerance: float = 0.1
    # Optional absolute tolerance in CFM; if provided, we use max(abs_tol, pct * sp)
    abs_cfm_tolerance: Optional[float] = None
    # Merge-asof tolerance in seconds
    merge_tolerance_seconds: int = 30


def compute_flow_tracking(
    df_flow: pd.DataFrame,
    df_flow_sp: Optional[pd.DataFrame],
    cfg: FlowTrackingConfig | None = None,
) -> dict:
    """
    Compare box flow vs flow setpoint over time.

    df_flow:    DataFrame with at least [timestamp_column, value_column]
    df_flow_sp: Optional DataFrame with the same columns for setpoint.

    Returns a dict with samples, within_band_pct, mean_error_cfm, mean_error_pct.
    """
    if cfg is None:
        cfg = FlowTrackingConfig()

    t_col = cfg.timestamp_column
    v_col = cfg.value_column

    if df_flow.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_cfm": None,
            "mean_error_pct": None,
        }

    # Normalise timestamps
    df_flow = df_flow.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_flow[t_col]):
        df_flow[t_col] = pd.to_datetime(df_flow[t_col])

    df_flow = df_flow.sort_values(t_col).rename(columns={v_col: "flow"})

    if df_flow_sp is None or df_flow_sp.empty:
        # No setpoint available; we cannot compute tracking.
        return {
            "samples": int(len(df_flow)),
            "within_band_pct": None,
            "mean_error_cfm": None,
            "mean_error_pct": None,
        }

    df_sp = df_flow_sp.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_sp[t_col]):
        df_sp[t_col] = pd.to_datetime(df_sp[t_col])

    df_sp = df_sp.sort_values(t_col).rename(columns={v_col: "flow_sp"})

    # Align using merge_asof (nearest neighbor within tolerance)
    merged = pd.merge_asof(
        df_flow,
        df_sp,
        on=t_col,
        direction="nearest",
        tolerance=pd.Timedelta(seconds=cfg.merge_tolerance_seconds),
    )

    # Drop rows where we failed to find a setpoint
    merged = merged.dropna(subset=["flow_sp"])
    if merged.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_cfm": None,
            "mean_error_pct": None,
        }

    merged["error_cfm"] = merged["flow"] - merged["flow_sp"]
    merged["error_pct"] = merged["error_cfm"] / merged["flow_sp"].where(
        merged["flow_sp"] != 0, pd.NA
    )

    # Tolerance: max(abs_cfm_tol, pct_tol * setpoint)
    pct_tol = cfg.pct_tolerance
    if cfg.abs_cfm_tolerance is not None:
        tol_cfm = merged["flow_sp"].abs() * pct_tol
        tol_cfm = tol_cfm.clip(lower=cfg.abs_cfm_tolerance)
    else:
        tol_cfm = merged["flow_sp"].abs() * pct_tol

    merged["within_band"] = merged["error_cfm"].abs() <= tol_cfm

    samples = int(len(merged))
    within_band_pct = float(merged["within_band"].mean() * 100.0)
    mean_error_cfm = float(merged["error_cfm"].mean())
    # Mean of absolute error percentage (ignore rows where setpoint==0)
    error_pct_valid = merged["error_pct"].dropna()
    mean_error_pct = float(error_pct_valid.abs().mean() * 100.0) if not error_pct_valid.empty else None

    return {
        "samples": samples,
        "within_band_pct": within_band_pct,
        "mean_error_cfm": mean_error_cfm,
        "mean_error_pct": mean_error_pct,
    }
