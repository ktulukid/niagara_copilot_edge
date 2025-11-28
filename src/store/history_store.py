from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..niagara_client.mqtt_history_ingest import HistorySample, niagara_canonical_name

# Max samples to keep per (station, history_id)
_MAX_PER_SERIES = 1000

# Key: (station_name, history_id) -> { timestamp -> HistorySample }
_store: Dict[Tuple[str, str], Dict[datetime, HistorySample]] = defaultdict(dict)


def clear() -> None:
    """Clear all stored history samples (mainly for tests)."""
    _store.clear()


def add_batch(samples: List[HistorySample]) -> None:
    """Add a batch of HistorySample objects into the in-memory store.

    For each (station, history_id, timestamp) we only keep the most recent
    value. Oldest timestamps are dropped once we exceed ``_MAX_PER_SERIES``
    per series.
    """
    if not samples:
        return

    # Track which series we touched so we can trim them once per batch.
    touched: set[Tuple[str, str]] = set()

    for s in samples:
        key = (
            niagara_canonical_name(s.station_name),
            niagara_canonical_name(s.history_id),
        )
        series = _store[key]
        series[s.timestamp] = s
        touched.add(key)

    # Enforce per-series cap.
    for key in touched:
        series = _store[key]
        if len(series) > _MAX_PER_SERIES:
            # Sort timestamps ascending and drop the oldest ones.
            timestamps = sorted(series.keys())
            to_drop = timestamps[:-_MAX_PER_SERIES]
            for ts in to_drop:
                del series[ts]


def _sample_to_json(sample: HistorySample) -> dict:
    """Convert a HistorySample to a JSON-serialisable dict."""
    d = asdict(sample)
    ts = d.get("timestamp")
    if isinstance(ts, datetime):
        d["timestamp"] = ts.isoformat()
    else:
        d["timestamp"] = str(ts)
    return d


def get_recent(
    *,
    station: Optional[str] = None,
    history_id: Optional[str] = None,
    limit: int = 100,
) -> List[dict]:
    """Return up to ``limit`` most recent samples.

    Filters:
      - ``station``: optional station name to filter on.
      - ``history_id``: optional history id to filter on.

    Returned list is sorted by timestamp ascending and already JSON-ready.
    """
    results: List[HistorySample] = []

    # Canonicalise filters once up front.
    station_key: Optional[str] = (
        niagara_canonical_name(station) if station is not None else None
    )
    history_key: Optional[str] = (
        niagara_canonical_name(history_id) if history_id is not None else None
    )

    for (st_name, hist_id), series in _store.items():
        if station_key is not None and st_name != station_key:
            continue
        if history_key is not None and hist_id != history_key:
            continue

        timestamps = sorted(series.keys())
        if limit <= 0:
            chosen_ts = timestamps
        else:
            chosen_ts = timestamps[-limit:]

        results.extend(series[ts] for ts in chosen_ts)

    # Global sort across all matching series (just in case)
    results.sort(key=lambda s: s.timestamp)

    if limit > 0 and len(results) > limit:
        results = results[-limit:]

    return [_sample_to_json(s) for s in results]
