# src/store/history_store.py

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict
from datetime import datetime
from typing import Deque, Dict, List, Optional, Tuple

from ..niagara_client.mqtt_history_ingest import HistorySample

# Max samples to keep per (station, history_id)
_MAX_PER_SERIES = 1000

# Key: (station_name, history_id)
_store: Dict[Tuple[str, str], Deque[HistorySample]] = defaultdict(deque)


def add_batch(samples: List[HistorySample]) -> None:
    """
    Add a batch of HistorySample objects into the in-memory store.
    Oldest samples are dropped when we exceed _MAX_PER_SERIES per series.
    """
    for s in samples:
        key = (s.station_name, s.history_id)
        dq = _store[key]
        dq.append(s)
        # Trim if over capacity
        while len(dq) > _MAX_PER_SERIES:
            dq.popleft()


def _sample_to_json(sample: HistorySample) -> dict:
    """
    Convert a HistorySample into a JSON-serializable dict.
    """
    d = asdict(sample)
    ts = d["timestamp"]
    if isinstance(ts, datetime):
        # ISO8601 with timezone offset, e.g. "2025-11-24T00:00:01.349-07:00"
        d["timestamp"] = ts.isoformat()
    else:
        d["timestamp"] = str(ts)
    return d


def get_recent(
    station: Optional[str] = None,
    history_id: Optional[str] = None,
    limit: int = 100,
) -> List[dict]:
    """
    Get up to `limit` most recent samples, optionally filtered
    by station and/or history_id.

    Returns a list of JSON-serializable dicts sorted by timestamp ascending.
    """
    results: List[HistorySample] = []

    for (st_name, hist_id), dq in _store.items():
        if station is not None and st_name != station:
            continue
        if history_id is not None and hist_id != history_id:
            continue

        # Take from the right (newest) but keep order oldest→newest
        if limit <= 0:
            # unlimited: take all
            results.extend(dq)
        else:
            results.extend(list(dq)[-limit:])

    # Sort across all series just in case
    results.sort(key=lambda s: s.timestamp)

    # If multiple series and limit is set, trim final list as well
    if limit > 0 and len(results) > limit:
        results = results[-limit:]

    return [_sample_to_json(s) for s in results]
