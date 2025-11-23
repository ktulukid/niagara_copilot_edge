from datetime import time
import pandas as pd

from ..config import ComfortConfig


def _parse_time(s: str) -> time:
    h, m = map(int, s.split(":"))
    return time(hour=h, minute=m)


def compute_zone_comfort(
    df: pd.DataFrame,
    comfort_cfg: ComfortConfig,
) -> dict:
    """
    df: history for one zone/equip, already filtered by time range.
    Returns a simple comfort score dict.
    """
    if df.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    c = comfort_cfg
    ts_col = c.timestamp_column
    sp_col = c.setpoint_column
    t_col = c.temp_column

    df = df.copy()
    df["__time"] = df[ts_col].dt.time

    occ_start = _parse_time(c.occupied_start)
    occ_end = _parse_time(c.occupied_end)

    occupied = df[(df["__time"] >= occ_start) & (df["__time"] <= occ_end)]

    if occupied.empty:
        return {
            "samples": 0,
            "within_band_pct": None,
            "mean_error_degF": None,
        }

    occupied["error"] = occupied[t_col] - occupied[sp_col]
    occupied["abs_error"] = occupied["error"].abs()

    total = len(occupied)
    within_band = (occupied["abs_error"] <= c.comfort_band_degF).sum()
    within_band_pct = within_band / total * 100.0
    mean_error = occupied["error"].mean()

    return {
        "samples": int(total),
        "within_band_pct": float(within_band_pct),
        "mean_error_degF": float(mean_error),
    }
