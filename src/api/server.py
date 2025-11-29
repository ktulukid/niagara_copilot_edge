from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import os
import traceback

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from ..config import AppConfig, ComfortConfig, load_config
from ..analytics.zone_pairs import zone_pairs_as_dicts, find_zone_pair
from ..analytics.zone_health import compute_zone_health, zone_health_to_dict
from ..analytics.flow import compute_flow_tracking, FlowTrackingConfig
from ..analytics.comfort import compute_zone_comfort
from ..niagara_client.mqtt_history_ingest import (
    HistorySample,
    make_history_mqtt_client,
)
from ..store import history_store, sqlite_store
from ..niagara_client.haystack_client import (
    HaystackHistoryClient,
    HaystackConfig as HSClientConfig,
)


app = FastAPI(title="Niagara Copilot Edge")

# ---- Global initialization -------------------------------------------------

_config: AppConfig = load_config()

# Initialize SQLite history store
sqlite_store.init(_config.db_path, _config.db_retention_hours)

# MQTT history ingestion → history_store + sqlite_store
try:

    def _on_history_batch(samples: List[HistorySample]) -> None:
        # In-memory store (debug)
        history_store.add_batch(samples)
        # Durable store
        sqlite_store.add_batch(samples)

    _mqtt_client = make_history_mqtt_client(
        mqtt_config=_config.mqtt,
        on_batch=_on_history_batch,
    )
except Exception as e:  # noqa: BLE001
    # We don't crash the API if MQTT fails; just log and continue.
    print(f"[warn] MQTT history client init failed: {e}")  # noqa: T201
    _mqtt_client = None

# Haystack client (optional, only if config is present)
_haystack_client: Optional[HaystackHistoryClient] = None
try:
    hs_cfg = None
    # Prefer top-level haystack config if present
    if getattr(_config, "haystack", None) is not None:
        hs_cfg = _config.haystack
    # Fallback to data_source.haystack if defined there
    elif getattr(_config.data_source, "haystack", None) is not None:
        hs_cfg = _config.data_source.haystack

    if hs_cfg is not None:
        password = os.getenv(hs_cfg.password_env, "")
        if not password:
            print(
                f"[warn] Haystack password env '{hs_cfg.password_env}' is empty or not set"
            )
        _haystack_client = HaystackHistoryClient(
            HSClientConfig(
                uri=hs_cfg.uri,
                username=hs_cfg.username,
                password=password,
                proj=hs_cfg.project,
            )
        )
        print("[info] Haystack client initialised")
    else:
        print("[info] No Haystack config found; Haystack client disabled")
except Exception as e:  # noqa: BLE001
    print(f"[warn] Haystack client init failed: {e}")  # noqa: T201
    _haystack_client = None


# ---- Pydantic models -------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"
    site_name: str


class HistorySampleJson(BaseModel):
    stationName: str
    historyId: str
    timestamp: datetime
    status: Optional[str] = None
    value: float


class ComfortMetricsModel(BaseModel):
    samples: int
    within_band_pct: Optional[float] = None
    mean_error_degF: Optional[float] = None


class ComfortZonePairResponse(BaseModel):
    station: str
    history_temp_id: str
    history_sp_id: str
    start: datetime
    end: datetime
    metrics: ComfortMetricsModel


class FlowTrackingMetricsModel(BaseModel):
    samples: int
    within_band_pct: Optional[float] = None
    mean_error_cfm: Optional[float] = None
    mean_error_pct: Optional[float] = None


class FlowTrackingResponse(BaseModel):
    station: str
    zone: str
    history_flow_id: Optional[str]
    history_flow_sp_id: Optional[str]
    start: datetime
    end: datetime
    metrics: FlowTrackingMetricsModel


class ZonePairResponse(BaseModel):
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


class ZoneHealthMetricsModel(BaseModel):
    # Identity
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

    # Comfort
    comfort_samples: int
    comfort_within_band_pct: Optional[float] = None
    comfort_mean_error_degF: Optional[float] = None

    # Flow
    flow_samples: int
    flow_within_band_pct: Optional[float] = None
    mean_flow_error_cfm: Optional[float] = None
    mean_flow_error_pct: Optional[float] = None

    # Damper
    damper_high_open_low_flow_pct: Optional[float] = None
    damper_closed_high_flow_pct: Optional[float] = None

    # Reheat
    reheat_waste_pct: Optional[float] = None

    # Fan
    fan_disagree_pct: Optional[float] = None
    fan_off_when_should_be_on_pct: Optional[float] = None
    fan_short_cycle_count: Optional[int] = None

    # Overall
    overall_score: Optional[float] = None

    # NEW: diagnostic status
    status: str
    reasons: List[str]


# ---- Utility helpers -------------------------------------------------------


def _rows_to_dataframe(rows: List[Dict[str, Any]]) -> Any:
    import pandas as pd

    if not rows:
        import pandas as _pd  # noqa: N812
        return _pd.DataFrame()
    df = pd.DataFrame(rows)
    if "ts" in df.columns:
        ts = pd.to_datetime(df["ts"], utc=True, format="mixed", errors="coerce")
        df["timestamp"] = ts.dt.tz_convert("UTC").dt.tz_localize(None)
        df = df.dropna(subset=["timestamp"])
        df = df.sort_values("timestamp")
    return df


def _normalize_for_json(obj: Any) -> Any:
    """
    Recursively normalize Haystack / hszinc types to plain Python types that
    FastAPI / Pydantic can serialize.

    - Basic types (str, int, float, bool, None) are returned as-is.
    - dict -> normalize values.
    - list/tuple -> normalize each element.
    - everything else -> str(obj).
    """
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: _normalize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize_for_json(v) for v in obj]
    # Fallback for MarkerType, Ref, Quantity, etc.
    return str(obj)


# ---- Basic health ----------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", site_name=_config.site_name)


# ---- Debug endpoints -------------------------------------------------------


@app.get("/debug/recent_memory", response_model=List[HistorySampleJson])
def debug_recent_memory(
    station: str = Query(..., description="Station name"),
    history_id: str = Query(..., description="History ID"),
    limit: int = Query(50, ge=1, le=1000),
) -> List[HistorySampleJson]:
    samples = history_store.get_recent(station, history_id, limit)
    return [
        HistorySampleJson(
            stationName=s.station_name,
            historyId=s.history_id,
            timestamp=s.timestamp,
            status=s.status,
            value=s.value,
        )
        for s in samples
    ]


@app.get("/debug/zone_pairs", response_model=List[ZonePairResponse])
def debug_zone_pairs(
    station: Optional[str] = Query(None),
    zone: Optional[str] = Query(None, description="Zone root filter (canonical)"),
) -> List[ZonePairResponse]:
    pairs_by_station = zone_pairs_as_dicts()
    results: List[ZonePairResponse] = []

    for st_name, zones in pairs_by_station.items():
        if station is not None and st_name != station:
            continue
        for zone_root, info in zones.items():
            if zone is not None and zone_root != zone:
                continue
            results.append(
                ZonePairResponse(
                    station=st_name,
                    zone_root=zone_root,
                    space_temp=info.get("space_temp"),
                    space_temp_sp=info.get("space_temp_sp"),
                    flow=info.get("flow"),
                    flow_sp=info.get("flow_sp"),
                    damper=info.get("damper"),
                    reheat=info.get("reheat"),
                    fan_cmd=info.get("fan_cmd"),
                    fan_status=info.get("fan_status"),
                )
            )

    return results


@app.get("/debug/comfort_zone_pair", response_model=ComfortZonePairResponse)
def debug_comfort_zone_pair(
    station: str = Query(...),
    temp_history_id: str = Query(...),
    sp_history_id: str = Query(...),
    hours: int = Query(24, ge=1, le=168),
    merge_tolerance_seconds: int = Query(
        30, ge=1, le=600, description="asof merge tolerance in seconds"
    ),
) -> ComfortZonePairResponse:
    from ..store import sqlite_store as _sqlite_store
    import pandas as pd

    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    rows_temp = _sqlite_store.query_series(station, temp_history_id, start, end)
    rows_sp = _sqlite_store.query_series(station, sp_history_id, start, end)
    df_temp = _rows_to_dataframe(rows_temp)
    df_sp = _rows_to_dataframe(rows_sp)

    if df_temp.empty or df_sp.empty:
        raise HTTPException(
            status_code=404,
            detail="No data for requested histories in the specified time range.",
        )

    # Build merged frame compatible with compute_zone_comfort
    merged = pd.merge_asof(
        df_temp.sort_values("timestamp").rename(columns={"value": "temp"}),
        df_sp.sort_values("timestamp").rename(columns={"value": "sp"}),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=merge_tolerance_seconds),
    ).dropna(subset=["temp", "sp"])

    if merged.empty:
        metrics = {"samples": 0, "within_band_pct": None, "mean_error_degF": None}
    else:
        # Pretend columns map to comfort config
        df_for_comfort = merged.rename(
            columns={"timestamp": "timestamp", "temp": "zn_t", "sp": "zn_sp"}
        )
        comfort_cfg: ComfortConfig = _config.comfort
        tmp_cfg = ComfortConfig(
            occupied_start=comfort_cfg.occupied_start,
            occupied_end=comfort_cfg.occupied_end,
            setpoint_column="zn_sp",
            temp_column="zn_t",
            timestamp_column="timestamp",
            equip_column=comfort_cfg.equip_column,
            comfort_band_degF=comfort_cfg.comfort_band_degF,
        )
        metrics = compute_zone_comfort(df_for_comfort, tmp_cfg)

    return ComfortZonePairResponse(
        station=station,
        history_temp_id=temp_history_id,
        history_sp_id=sp_history_id,
        start=start,
        end=end,
        metrics=ComfortMetricsModel(**metrics),
    )


@app.get("/debug/flow_tracking", response_model=FlowTrackingResponse)
def debug_flow_tracking(
    station: str = Query(...),
    zone: str = Query(..., description="Zone root (canonical)"),
    hours: int = Query(24, ge=1, le=168),
) -> FlowTrackingResponse:
    from ..store import sqlite_store as _sqlite_store

    pairs_by_station = zone_pairs_as_dicts()
    zone_info = find_zone_pair(pairs_by_station, station, zone)
    if zone_info is None:
        raise HTTPException(status_code=404, detail="Zone not found for station.")

    flow_id = zone_info.get("flow")
    flow_sp_id = zone_info.get("flow_sp")

    if not flow_id:
        raise HTTPException(
            status_code=404, detail="Zone has no flow history configured."
        )

    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    rows_flow = _sqlite_store.query_series(station, flow_id, start, end)
    df_flow = _rows_to_dataframe(rows_flow)

    if flow_sp_id:
        rows_flow_sp = _sqlite_store.query_series(station, flow_sp_id, start, end)
        df_flow_sp = _rows_to_dataframe(rows_flow_sp)
    else:
        df_flow_sp = _rows_to_dataframe([])

    cfg = FlowTrackingConfig()
    cfg.timestamp_column = "timestamp"
    cfg.value_column = "value"
    cfg.merge_tolerance_seconds = 30

    metrics = compute_flow_tracking(df_flow, df_flow_sp, cfg)

    return FlowTrackingResponse(
        station=station,
        zone=zone,
        history_flow_id=flow_id,
        history_flow_sp_id=flow_sp_id,
        start=start,
        end=end,
        metrics=FlowTrackingMetricsModel(
            samples=metrics.get("samples", 0),
            within_band_pct=metrics.get("within_band_pct"),
            mean_error_cfm=metrics.get("mean_error_cfm"),
            mean_error_pct=metrics.get("mean_error_pct"),
        ),
    )


# ---- Summary endpoints -----------------------------------------------------


@app.get("/summary/zone_health", response_model=ZoneHealthMetricsModel)
def summary_zone_health(
    station: str = Query(...),
    zone: str = Query(..., description="Zone root (canonical)"),
    hours: int = Query(24, ge=1, le=168),
) -> ZoneHealthMetricsModel:
    pairs_by_station = zone_pairs_as_dicts()
    zone_info = find_zone_pair(pairs_by_station, station, zone)
    if zone_info is None:
        raise HTTPException(status_code=404, detail="Zone not found for station.")

    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    metrics = compute_zone_health(
        station=station,
        zone_root=zone,
        zone_info=zone_info,
        comfort_cfg=_config.comfort,
        start=start,
        end=end,
    )

    return ZoneHealthMetricsModel(**zone_health_to_dict(metrics))


@app.get("/summary/building_health", response_model=List[ZoneHealthMetricsModel])
def summary_building_health(
    station: str = Query(...),
    hours: int = Query(24, ge=1, le=168),
) -> List[ZoneHealthMetricsModel]:
    pairs_by_station = zone_pairs_as_dicts()
    zones = pairs_by_station.get(station)
    if not zones:
        raise HTTPException(status_code=404, detail="No zones found for station.")

    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    results: List[ZoneHealthMetricsModel] = []

    for zone_root, zone_info in zones.items():
        metrics = compute_zone_health(
            station=station,
            zone_root=zone_root,
            zone_info=zone_info,
            comfort_cfg=_config.comfort,
            start=start,
            end=end,
        )
        results.append(ZoneHealthMetricsModel(**zone_health_to_dict(metrics)))

    # Sort worst first: primary by status, secondary by overall score ascending
    status_order = {"critical": 0, "warning": 1, "ok": 2, "no_data": 3}

    def sort_key(m: ZoneHealthMetricsModel) -> Any:
        return (
            status_order.get(m.status, 3),
            float("inf") if m.overall_score is None else m.overall_score,
        )

    results.sort(key=sort_key)
    return results


# ---- Haystack test endpoints ----------------------------------------------


@app.get("/haystack/test/zoneTemps")
def haystack_test_zone_temps(
    site_ref: Optional[str] = Query(
        default=None, description="Optional siteRef id (without @)"
    )
) -> Dict[str, Any]:
    if _haystack_client is None:
        raise HTTPException(
            status_code=500, detail="Haystack client not initialised or disabled."
        )

    try:
        points = _haystack_client.find_zone_temp_points(site_ref=site_ref, limit=500)
        points_norm = _normalize_for_json(points)
        return {"count": len(points_norm), "points": points_norm}
    except Exception as e:  # noqa: BLE001
        tb = traceback.format_exc()
        print("[error] haystack_test_zoneTemps failed:\n", tb)  # noqa: T201
        raise HTTPException(status_code=500, detail=f"Haystack error: {e}")


@app.get("/haystack/test/history")
def haystack_test_history(
    id: str = Query(..., description="Haystack id, e.g. @vav-3-04-zn-t"),
    range: str = Query(
        "today",
        description="Haystack range string, e.g. 'today' or '2025-11-27,2025-11-28'",
    ),
) -> Dict[str, Any]:
    if _haystack_client is None:
        raise HTTPException(
            status_code=500, detail="Haystack client not initialised or disabled."
        )

    try:
        samples = _haystack_client.his_read(id, range)
        samples_norm = [
            {"ts": ts.isoformat(), "val": float(val)} for ts, val in samples
        ]
        return {
            "id": id,
            "range": range,
            "samples": samples_norm,
        }
    except Exception as e:  # noqa: BLE001
        tb = traceback.format_exc()
        print("[error] haystack_test_history failed:\n", tb)  # noqa: T201
        raise HTTPException(status_code=500, detail=f"Haystack error: {e}")
