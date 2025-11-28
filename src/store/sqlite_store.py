from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

from ..niagara_client.mqtt_history_ingest import HistorySample, niagara_canonical_name

# Single process-wide connection; FastAPI runs in a single process in this app.
_conn: Optional[sqlite3.Connection] = None
_db_path: Optional[Path] = None
_retention_hours: int = 24 * 30  # default 30 days


def init(db_path: str | Path, retention_hours: int) -> None:
    """Initialise the SQLite history store.

    Creates the database file and tables if needed and stores the connection
    for reuse by helper functions.
    """
    global _conn, _db_path, _retention_hours

    _db_path = Path(db_path)
    _retention_hours = int(retention_hours)

    _db_path.parent.mkdir(parents=True, exist_ok=True)

    _conn = sqlite3.connect(_db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA synchronous=NORMAL")

    _init_schema(_conn)


def _get_conn() -> sqlite3.Connection:
    if _conn is None:
        raise RuntimeError("SQLite store not initialised. Call sqlite_store.init() first.")
    return _conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS history_samples (
            station_key     TEXT NOT NULL,
            history_key     TEXT NOT NULL,
            station_name    TEXT NOT NULL,
            history_id      TEXT NOT NULL,
            ts              TEXT NOT NULL,
            status          TEXT,
            value           REAL NOT NULL,
            PRIMARY KEY (station_key, history_key, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_history_samples_ts
        ON history_samples(ts)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_history_samples_series
        ON history_samples(station_key, history_key, ts)
        """
    )
    conn.commit()


def add_batch(samples: List[HistorySample]) -> None:
    """Insert a batch of HistorySample rows into SQLite.

    Uses a UNIQUE constraint on (station_key, history_key, ts) so repeated
    publishes of the same history window simply overwrite existing rows
    instead of duplicating them.
    """
    if not samples:
        return

    conn = _get_conn()

    rows: List[Tuple[str, str, str, str, str, str, float]] = []
    for s in samples:
        station_key = niagara_canonical_name(s.station_name)
        history_key = niagara_canonical_name(s.history_id)
        ts_iso = s.timestamp.astimezone(timezone.utc).isoformat()
        rows.append(
            (
                station_key,
                history_key,
                s.station_name,
                s.history_id,
                ts_iso,
                s.status,
                float(s.value),
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO history_samples (
            station_key, history_key, station_name, history_id,
            ts, status, value
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    _apply_retention(conn)


def _apply_retention(conn: sqlite3.Connection) -> None:
    """Enforce a rolling history window based on _retention_hours."""
    if _retention_hours <= 0:
        return

    cutoff = datetime.now(timezone.utc) - timedelta(hours=_retention_hours)
    cutoff_iso = cutoff.isoformat()

    conn.execute(
        "DELETE FROM history_samples WHERE ts < ?",
        (cutoff_iso,),
    )
    conn.commit()


def list_series(limit: int = 1000) -> List[Dict[str, Any]]:
    """Return distinct (station, history) pairs seen so far."""
    conn = _get_conn()
    cur = conn.execute(
        """
        SELECT DISTINCT
            station_key,
            history_key,
            station_name,
            history_id
        FROM history_samples
        ORDER BY station_name, history_id
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    return [
        {
            "station_key": r[0],
            "history_key": r[1],
            "stationName": r[2],
            "historyId": r[3],
        }
        for r in rows
    ]


def query_series(
    station: Optional[str] = None,
    history_id: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Query time-series samples from SQLite.

    All filters are optional; when both station and history_id are provided,
    the query is restricted to that single series.
    """
    conn = _get_conn()

    where_clauses: List[str] = []
    params: List[Any] = []

    if station is not None:
        where_clauses.append("station_key = ?")
        params.append(niagara_canonical_name(station))

    if history_id is not None:
        where_clauses.append("history_key = ?")
        params.append(niagara_canonical_name(history_id))

    if start is not None:
        where_clauses.append("ts >= ?")
        params.append(start.astimezone(timezone.utc).isoformat())

    if end is not None:
        where_clauses.append("ts <= ?")
        params.append(end.astimezone(timezone.utc).isoformat())

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT
            station_name,
            history_id,
            ts,
            status,
            value
        FROM history_samples
        {where_sql}
        ORDER BY ts ASC
        LIMIT ?
    """

    params.append(limit)

    cur = conn.execute(sql, params)
    rows = cur.fetchall()

    result: List[Dict[str, Any]] = []
    for station_name, hist_id, ts, status, value in rows:
        result.append(
            {
                "stationName": station_name,
                "historyId": hist_id,
                "timestamp": ts,
                "status": status,
                "value": value,
            }
        )

    return result
