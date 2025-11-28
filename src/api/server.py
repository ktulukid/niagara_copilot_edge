# src/api/server.py

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..config import load_config, AppConfig
from ..niagara_client.factory import make_history_client
from ..analytics.comfort import compute_zone_comfort
from ..niagara_client.mqtt_history_ingest import (
    make_history_mqtt_client,
    HistorySample,
    niagara_canonical_name,
)
from ..store.history_store import add_batch as mem_add_batch, get_recent as mem_get_recent
from ..store import sqlite_store
from ..analytics.zone_pairs import zone_pairs_as_dicts, find_zone_pair
from ..analytics.flow import compute_flow_tracking, FlowTrackingConfig
from ..analytics.zone_health import compute_zone_health, zone_health_to_dict


# -----------------------------------------------------------------------------
# App + global init
# -----------------------------------------------------------------------------

app = FastAPI(title="Niagara Copilot Edge")
router = APIRouter()

_config: AppConfig = load_config()

# Optional HTTP/CSV/oBIX history client (currently None for MQTT-only mode)
try:
    _history_client = make_history_client(_config)
except Exception as exc:  # pragma: no cover
    print(f"[warn] history client init failed: {exc}")
    _history_client = None

# Initialise SQLite history store
try:
    sqlite_store.init(_config.db_path, _config.db_retention_hours)
    print(
        f"[sqlite] history store initialised at {_config.db_path} "
        f"(retention={_config.db_retention_hours} hours)"
    )
except Exception as exc:  # pragma: no cover
    print(f"[warn] sqlite_store.init failed: {exc}")


def _on_history_batch(samples: List[HistorySample]) -> None:
    """
    Callback for Niagara history MQTT client.

    Writes to both the in-memory history_store and the SQLite-backed store.
    """
    if not samples:
        return

    # In-memory store (for quick inspection / debugging)
    mem_add_batch(samples)

    # SQLite store (for durable analytics)
    sqlite_store.add_batch(samples)


# Start MQTT client for Niagara history ingestion
try:
    mqtt_host = os.getenv("MQTT_HOST", _config.mqtt.host)
    mqtt_port = int(os.getenv("MQTT_PORT", str(_config.mqtt.port)))
    history_topic = _config.mqtt.history_topic

    _mqtt_client = make_history_mqtt_client(
        broker_host=mqtt_host,
        broker_port=mqtt_port,
        topic=history_topic,
        on_batch=_on_history_batch,
    )
    _mqtt_client.loop_start()
    print(f"[mqtt] Niagara history MQTT client started on {mqtt_host}:{mqtt_port} topic={history_topic}")
except Exception as exc:  # pragma: no cover
    print(f"[warn] failed to start Niagara history MQTT client: {exc}")


# -----------------------------------------------------------------------------
# Pydantic models
# -----------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str = "ok"


class HistorySampleJson(BaseModel):
    stationName: str
    historyId: str
    timestamp: str
    status: Optional[str] = None
    value: float


class ComfortMetrics(BaseModel):
    samples: int
    within_band_pct: Optional[float] = None
    mean_error_degF: Optional[float] = None


class ComfortZonePairResponse(BaseModel):
    history_temp_id: str
    history_sp_id: str
    metrics: ComfortMetrics


class ZonePairResponse(BaseModel):
    station_key: str
    station_name: str
    zone_root: str
    space_temp: str | None = None
    space_temp_sp: str | None = None
    flow: str | None = None
    flow_sp: str | None = None
    damper: str | None = None
    reheat: str | None = None
    fan_cmd: str | None = None
    fan_status: str | None = None


class FlowTrackingResponse(BaseModel):
    station: str
    zone: str
    flow_history_id: str | None
    flow_sp_history_id: str | None
    start: datetime | None
    end: datetime | None
    metrics: Dict[str, Any]


class ZoneHealthMetricsModel(BaseModel):
    station: str
    zone_root: str
    space_temp: str | None
    space_temp_sp: str | None
    flow: str | None
    flow_sp: str | None
    damper: str | None
    reheat: str | None
    fan_cmd: str | None
    fan_status: str | None

    comfort_samples: int
    comfort_within_band_pct: float | None
    comfort_mean_error_degF: float | None

    flow_samples: int
    flow_within_band_pct: float | None

    damper_high_open_low_flow_pct: float | None
    damper_closed_high_flow_pct: float | None

    reheat_waste_pct: float | None
    overall_score: float | None


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def _rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["timestamp", "value"])
    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse()


@router.get("/debug/recent_memory", response_model=List[HistorySampleJson])
def debug_recent_memory(
    station: Optional[str] = Query(None),
    history_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=10_000),
) -> List[HistorySampleJson]:
    """
    Inspect the most recent samples in the in-memory history_store.
    """
    results = mem_get_recent(station=station, history_id=history_id, limit=limit)
    return [HistorySampleJson(**row) for row in results]


@router.get("/debug/comfort_zone_pair", response_model=ComfortZonePairResponse)
def debug_comfort_zone_pair(
    station: str = Query(..., description="Station name, e.g. 'AmsShop'"),
    temp_history_id: str = Query(..., description="HistoryId for space temperature"),
    sp_history_id: str = Query(..., description="HistoryId for effective setpoint"),
    hours: int = Query(24, ge=1, le=72),
    merge_tolerance_seconds: int = Query(30, ge=1, le=300),
) -> ComfortZonePairResponse:
    """
    Debug endpoint: compute comfort metrics for a temp + setpoint history pair
    over the last N hours, using nearest-timestamp join with a tolerance.

    Data source is the SQLite history_samples table.
    """
    now = datetime.utcnow()
    start = now - timedelta(hours=hours)

    rows_temp = sqlite_store.query_series(
        station=station,
        history_id=temp_history_id,
        start=start,
        end=now,
        limit=10_000,
    )
    rows_sp = sqlite_store.query_series(
        station=station,
        history_id=sp_history_id,
        start=start,
        end=now,
        limit=10_000,
    )

    df_temp = _rows_to_dataframe(rows_temp)
    df_sp = _rows_to_dataframe(rows_sp)

    if df_temp.empty or df_sp.empty:
        metrics = ComfortMetrics(samples=0, within_band_pct=None, mean_error_degF=None)
        return ComfortZonePairResponse(
            history_temp_id=temp_history_id,
            history_sp_id=sp_history_id,
            metrics=metrics,
        )

    comfort_cfg = _config.comfort
    ts_col = comfort_cfg.timestamp_column
    t_col = comfort_cfg.temp_column
    sp_col = comfort_cfg.setpoint_column

    df_temp = df_temp.rename(columns={"timestamp": ts_col, "value": t_col})
    df_sp = df_sp.rename(columns={"timestamp": ts_col, "value": sp_col})

    df_temp = df_temp.sort_values(ts_col)
    df_sp = df_sp.sort_values(ts_col)

    merged = pd.merge_asof(
        df_temp,
        df_sp[[ts_col, sp_col]],
        on=ts_col,
        direction="nearest",
        tolerance=pd.Timedelta(seconds=merge_tolerance_seconds),
    )

    merged = merged.dropna(subset=[sp_col])
    metrics_dict = compute_zone_comfort(merged, comfort_cfg)
    metrics = ComfortMetrics(**metrics_dict)

    return ComfortZonePairResponse(
        history_temp_id=temp_history_id,
        history_sp_id=sp_history_id,
        metrics=metrics,
    )


@router.get("/debug/zone_pairs", response_model=List[ZonePairResponse])
def debug_zone_pairs(
    station: str | None = Query(None, description="Filter by stationName (optional)"),
    zone: str | None = Query(None, description="Filter by zone label, e.g. 'VAV 1-01' (optional)"),
) -> List[ZonePairResponse]:
    """
    Inspect auto-discovered zone pairings (temp, flow, damper, reheat, fan) for all equipment.
    """
    all_pairs = zone_pairs_as_dicts()

    if station:
        station_key = niagara_canonical_name(station)
        all_pairs = [p for p in all_pairs if p.get("station_key") == station_key]

    if zone:
        z = zone.lower().replace("_", "-").replace(" ", "-").strip("-")
        all_pairs = [p for p in all_pairs if p.get("zone_root") == z]

    return [ZonePairResponse(**p) for p in all_pairs]


@router.get("/debug/flow_tracking", response_model=FlowTrackingResponse)
def debug_flow_tracking(
    station: str = Query(..., description="Station name, e.g. 'AmsShop'"),
    zone: str = Query(..., description="Zone label, e.g. 'VAV 1-01'"),
    start: datetime | None = Query(None, description="Start time (ISO-8601, optional)"),
    end: datetime | None = Query(None, description="End time (ISO-8601, optional)"),
) -> FlowTrackingResponse:
    """
    Flow vs Flow Setpoint tracking for a single zone, using auto pairing.
    """
    station_key = niagara_canonical_name(station)
    zone_root = zone.lower().replace("_", "-").replace(" ", "-").strip("-")

    zp = find_zone_pair(station_key=station_key, zone_root=zone_root)
    if zp is None:
        raise HTTPException(
            status_code=404,
            detail=f"No auto-paired zone found for station={station!r}, zone_root={zone_root!r}",
        )

    flow_hist = zp.flow
    flow_sp_hist = zp.flow_sp

    if flow_hist is None:
        raise HTTPException(
            status_code=404,
            detail="No flow historyId detected for this zone.",
        )

    now = datetime.utcnow()
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)

    rows_flow = sqlite_store.query_series(
        station=station,
        history_id=flow_hist,
        start=start,
        end=end,
        limit=10_000,
    )
    df_flow = _rows_to_dataframe(rows_flow)

    df_sp = None
    if flow_sp_hist is not None:
        rows_sp = sqlite_store.query_series(
            station=station,
            history_id=flow_sp_hist,
            start=start,
            end=end,
            limit=10_000,
        )
        df_sp = _rows_to_dataframe(rows_sp)

    if df_flow.empty:
        metrics = compute_flow_tracking(
            df_flow=pd.DataFrame(columns=["timestamp", "value"]),
            df_flow_sp=None,
            cfg=FlowTrackingConfig(),
        )
    else:
        df_flow = df_flow.rename(columns={"timestamp": "timestamp", "value": "value"})
        if df_sp is not None and not df_sp.empty:
            df_sp = df_sp.rename(columns={"timestamp": "timestamp", "value": "value"})
        else:
            df_sp = None

        metrics = compute_flow_tracking(df_flow, df_sp, cfg=FlowTrackingConfig())

    return FlowTrackingResponse(
        station=station,
        zone=zone,
        flow_history_id=flow_hist,
        flow_sp_history_id=flow_sp_hist,
        start=start,
        end=end,
        metrics=metrics,
    )


@router.get("/summary/zone_health", response_model=ZoneHealthMetricsModel)
def summary_zone_health(
    station: str = Query(..., description="Station name, e.g. 'AmsShop'"),
    zone: str = Query(..., description="Zone label, e.g. 'VAV 1-01' or 'FPB 3-01'"),
    hours: int = Query(24, ge=1, le=72),
) -> ZoneHealthMetricsModel:
    """
    Full health metrics for a single zone/equipment:
      - comfort
      - flow tracking
      - damper sanity
      - reheat waste
      - overall score
    """
    station_key = niagara_canonical_name(station)
    zone_root = zone.lower().replace("_", "-").replace(" ", "-").strip("-")

    # Find the zone pair entry
    pairs = zone_pairs_as_dicts()
    zp = None
    for p in pairs:
        if p.get("station_key") == station_key and p.get("zone_root") == zone_root:
            zp = p
            break

    if zp is None:
        raise HTTPException(
            status_code=404,
            detail=f"No zone pair metadata found for station={station!r}, zone_root={zone_root!r}",
        )

    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    metrics = compute_zone_health(
        station=station,
        zone_root=zone_root,
        zone_info=zp,
        comfort_cfg=_config.comfort,
        start=start,
        end=end,
    )
    mdict = zone_health_to_dict(metrics)

    return ZoneHealthMetricsModel(
        station=station,
        zone_root=mdict["zone_root"],
        space_temp=mdict["space_temp"],
        space_temp_sp=mdict["space_temp_sp"],
        flow=mdict["flow"],
        flow_sp=mdict["flow_sp"],
        damper=mdict["damper"],
        reheat=mdict["reheat"],
        fan_cmd=mdict["fan_cmd"],
        fan_status=mdict["fan_status"],
        comfort_samples=mdict["comfort_samples"],
        comfort_within_band_pct=mdict["comfort_within_band_pct"],
        comfort_mean_error_degF=mdict["comfort_mean_error_degF"],
        flow_samples=mdict["flow_samples"],
        flow_within_band_pct=mdict["flow_within_band_pct"],
        damper_high_open_low_flow_pct=mdict["damper_high_open_low_flow_pct"],
        damper_closed_high_flow_pct=mdict["damper_closed_high_flow_pct"],
        reheat_waste_pct=mdict["reheat_waste_pct"],
        overall_score=mdict["overall_score"],
    )


@router.get("/summary/building_health", response_model=List[ZoneHealthMetricsModel])
def summary_building_health(
    station: str = Query(..., description="Station name, e.g. 'AmsShop'"),
    hours: int = Query(24, ge=1, le=72),
) -> List[ZoneHealthMetricsModel]:
    """
    24-hour (or N-hour) health summary for all zones/equipment in a station.
    Sorted by overall_score ascending (worst first).
    """
    station_key = niagara_canonical_name(station)
    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    pairs = zone_pairs_as_dicts()
    station_pairs = [p for p in pairs if p.get("station_key") == station_key]

    results: List[ZoneHealthMetricsModel] = []

    for zp in station_pairs:
        zone_root = zp.get("zone_root")
        metrics = compute_zone_health(
            station=station,
            zone_root=zone_root,
            zone_info=zp,
            comfort_cfg=_config.comfort,
            start=start,
            end=end,
        )
        mdict = zone_health_to_dict(metrics)

        model = ZoneHealthMetricsModel(
            station=station,
            zone_root=mdict["zone_root"],
            space_temp=mdict["space_temp"],
            space_temp_sp=mdict["space_temp_sp"],
            flow=mdict["flow"],
            flow_sp=mdict["flow_sp"],
            damper=mdict["damper"],
            reheat=mdict["reheat"],
            fan_cmd=mdict["fan_cmd"],
            fan_status=mdict["fan_status"],
            comfort_samples=mdict["comfort_samples"],
            comfort_within_band_pct=mdict["comfort_within_band_pct"],
            comfort_mean_error_degF=mdict["comfort_mean_error_degF"],
            flow_samples=mdict["flow_samples"],
            flow_within_band_pct=mdict["flow_within_band_pct"],
            damper_high_open_low_flow_pct=mdict["damper_high_open_low_flow_pct"],
            damper_closed_high_flow_pct=mdict["damper_closed_high_flow_pct"],
            reheat_waste_pct=mdict["reheat_waste_pct"],
            overall_score=mdict["overall_score"],
        )
        results.append(model)

    # Sort by overall_score (worst first); None scores go to the bottom
    results.sort(
        key=lambda m: (m.overall_score is None, m.overall_score if m.overall_score is not None else 999.0)
    )

    return results


# Mount router
app.include_router(router)
