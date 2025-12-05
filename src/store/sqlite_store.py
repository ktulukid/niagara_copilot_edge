from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..niagara_client.mqtt_history_ingest import HistorySample


# Path to DB and retention policy (configured via init)
_db_path: Optional[str] = None
_retention_hours: int = 24

# Shared connection (simple pattern for this edge app)
_conn: Optional[sqlite3.Connection] = None

# In-memory series metadata keyed by (station_name, history_id)
# This is where we attach equipment / floor / point_name / unit / tags
_series_meta: Dict[Tuple[str, str], Dict[str, Any]] = {}


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        if _db_path is None:
            raise RuntimeError("sqlite_store.init() must be called before use")
        _conn = sqlite3.connect(_db_path, isolation_level=None, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL;")
        _conn.execute("PRAGMA synchronous=NORMAL;")
    return _conn


def _init_schema() -> None:
    """
    Create the history_samples table and indexes.

    NOTE: This drops any existing history_samples table to avoid
    schema-mismatch issues while we iterate on the design.
    """
    conn = _get_conn()

    # Blow away any legacy schema; this is an edge cache, so we can repopulate.
    conn.execute("DROP TABLE IF EXISTS history_samples;")

    conn.execute(
        """
        CREATE TABLE history_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station TEXT NOT NULL,
            history_id TEXT NOT NULL,
            ts_utc TEXT NOT NULL,   -- ISO 8601, UTC
            value REAL NOT NULL,
            status TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_history_samples_station_hist_ts
        ON history_samples (station, history_id, ts_utc);
        """
    )


def init(db_path: str, retention_hours: int) -> None:
    """
    Initialise the SQLite store.

    - db_path: filesystem path to SQLite file.
    - retention_hours: how long to retain data before pruning.
    """
    global _db_path, _retention_hours, _conn, _series_meta
    _db_path = db_path
    _retention_hours = int(retention_hours)
    _conn = None  # force reconnect with new path
    _series_meta = {}
    _init_schema()


def _to_utc_iso(ts: datetime) -> str:
    """
    Convert a datetime (aware or naive) to a UTC ISO-8601 string
    suitable for storage in ts_utc.
    """
    if ts.tzinfo is None:
        # Assume already UTC if naive; this matches previous behaviour
        ts_utc = ts.replace(tzinfo=timezone.utc)
    else:
        ts_utc = ts.astimezone(timezone.utc)
    return ts_utc.isoformat()


def _prune_old_rows() -> None:
    """
    Delete rows older than retention_hours from history_samples.
    Called opportunistically from add_batch().
    """
    if _retention_hours <= 0:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_retention_hours)
    cutoff_iso = cutoff.isoformat()
    conn = _get_conn()
    conn.execute(
        "DELETE FROM history_samples WHERE ts_utc < ?;",
        (cutoff_iso,),
    )


def add_batch(samples: Iterable[HistorySample]) -> None:
    """
    Insert a batch of HistorySample into SQLite and update in-memory metadata.

    Called from mqtt_history_ingest._on_mqtt_message.
    """
    samples = list(samples)
    if not samples:
        return

    conn = _get_conn()
    rows: List[Tuple[str, str, str, float, Optional[str]]] = []

    for s in samples:
        ts_iso = _to_utc_iso(s.timestamp)
        rows.append(
            (
                s.station_name,
                s.history_id,
                ts_iso,
                float(s.value),
                s.status,
            )
        )

        # Update in-memory series metadata for this (station, history_id)
        key = (s.station_name, s.history_id)
        meta = _series_meta.get(key) or {}
        # Only overwrite fields when new non-None values arrive
        if s.equipment is not None:
            meta["equipment"] = s.equipment
        if s.floor is not None:
            meta["floor"] = s.floor
        if s.point_name is not None:
            meta["point_name"] = s.point_name
        if s.unit is not None:
            meta["unit"] = s.unit
        if s.tags is not None:
            meta["tags"] = list(s.tags)
        _series_meta[key] = meta

    conn.executemany(
        """
        INSERT INTO history_samples (
            station, history_id, ts_utc, value, status
        ) VALUES (?, ?, ?, ?, ?);
        """,
        rows,
    )

    _prune_old_rows()


def list_series(limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Return a list of distinct (station, history_id) pairs, with any
    known metadata (equipment, floor, point_name, unit, tags) attached.

    Shape of each entry:
        {
            "station": "AmsShop",
            "history_id": "/AmsShop/Vav1_01$20SpaceTemperature",
            "equipment": "VAV 1-01",      # if known
            "floor": "First",             # if known
            "point_name": "SpaceTemperature",  # if known
            "unit": "°F",                 # if known
            "tags": [...],                # if known
        }
    """
    conn = _get_conn()
    cur = conn.execute(
        """
        SELECT DISTINCT station, history_id
        FROM history_samples
        ORDER BY station, history_id
        LIMIT ?;
        """,
        (limit,),
    )
    rows = cur.fetchall()

    series: List[Dict[str, Any]] = []
    for station, history_id in rows:
        key = (station, history_id)
        meta = _series_meta.get(key, {})
        entry: Dict[str, Any] = {
            "station": station,
            "history_id": history_id,
        }
        # Attach any known metadata from in-memory index
        entry.update({k: v for k, v in meta.items() if v is not None})
        series.append(entry)

    return series


def query_series(
    station: str,
    history_id: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    """
    Query time-series samples for a given station + history_id in a
    [start, end] time window.

    Returns a list of dicts with at least:
        {
            "stationName": <station>,
            "historyId": <history_id>,
            "ts": <ISO UTC string>,
            "value": <float>,
            "status": <str or None>,
        }
    """
    start_iso = _to_utc_iso(start)
    end_iso = _to_utc_iso(end)
    conn = _get_conn()
    cur = conn.execute(
        """
        SELECT station, history_id, ts_utc, value, status
        FROM history_samples
        WHERE station = ?
          AND history_id = ?
          AND ts_utc >= ?
          AND ts_utc <= ?
        ORDER BY ts_utc;
        """,
        (station, history_id, start_iso, end_iso),
    )
    rows = cur.fetchall()

    results: List[Dict[str, Any]] = []
    for station_val, hist_val, ts_utc, value, status in rows:
        results.append(
            {
                "stationName": station_val,
                "historyId": hist_val,
                "ts": ts_utc,
                "value": float(value),
                "status": status,
            }
        )

    return results
