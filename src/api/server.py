import pandas as pd
import os

from datetime import datetime, timedelta
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import List
from ..config import load_config
from ..niagara_client.factory import make_history_client
from ..analytics.comfort import compute_zone_comfort
from ..niagara_client.mqtt_history_ingest import make_history_mqtt_client, HistorySample
from ..store.history_store import add_batch, get_recent


app = FastAPI(title="Niagara Copilot Edge")

_config = load_config()
try:
    _history_client = make_history_client(_config)
except Exception as e:
    print(f"[warn] history client init failed: {e}")
    _history_client = None
mqtt_client = None  # will hold the paho client instance


def handle_history_batch(samples: List[HistorySample]) -> None:
    """
    Called for each MQTT history JSON message.

    - Stores samples in the in-memory history store.
    - Logs a summary line.
    """
    if not samples:
        return

    # Store in memory
    add_batch(samples)

    # Log summary
    first = samples[0]
    print(
        f"[mqtt] got {len(samples)} samples for "
        f"{first.station_name} {first.history_id}"
    )



class ComfortResponse(BaseModel):
    site: str
    equip: str
    start: datetime
    end: datetime
    samples: int
    within_band_pct: float
    mean_error_degF: float


@app.get("/comfort/zone", response_model=ComfortResponse)
def get_zone_comfort(
    equip: str = Query(..., description="Equipment/zone identifier"),
    hours: int = Query(24, ge=1, le=168, description="Hours back from now"),
):
    end = datetime.utcnow()
    start = end - timedelta(hours=hours)

    df = _history_client.get_zone_history(equip=equip, start=start, end=end)
    metrics = compute_zone_comfort(df, _config.comfort)

    return ComfortResponse(
        site=_config.site_name,
        equip=equip,
        start=start,
        end=end,
        samples=metrics["samples"],
        within_band_pct=metrics["within_band_pct"],
        mean_error_degF=metrics["mean_error_degF"],
    )
@app.on_event("startup")
def start_mqtt_ingestion() -> None:
    """
    Start background MQTT subscription to niagara/histories.

    If the broker is not reachable, log a warning and continue
    without MQTT ingestion so the API still starts.
    """
    global mqtt_client

    # Prefer config.yaml, allow env overrides if you like
    host = os.getenv("MQTT_BROKER_HOST", _config.mqtt.host)
    port = int(os.getenv("MQTT_BROKER_PORT", str(_config.mqtt.port)))
    topic = os.getenv("MQTT_TOPIC", _config.mqtt.history_topic)

    try:
        mqtt_client = make_history_mqtt_client(
            broker_host=host,
            broker_port=port,
            topic=topic,
            on_batch=handle_history_batch,
        )
        mqtt_client.loop_start()
        print(f"[mqtt] ingestion started on {host}:{port}, topic={topic}")
    except Exception as e:
        print(f"[warn] MQTT ingestion disabled (could not connect to {host}:{port}): {e}")
        mqtt_client = None

@app.get("/debug/histories")
def debug_get_histories(
    stationName: str | None = Query(None, description="Filter by stationName"),
    historyId: str | None = Query(None, description="Filter by historyId"),
    limit: int = Query(100, ge=1, le=1000, description="Max samples to return"),
):
    """
    Debug endpoint to inspect recent history samples received via MQTT.

    Example:
      /debug/histories?stationName=AmsShop&historyId=/AmsShop/MaxSpaceTemp&limit=50
    """
    items = get_recent(
        station=stationName,
        history_id=historyId,
        limit=limit,
    )

    if not items:
        # Not an error, but useful feedback while debugging
        return {
            "stationName": stationName,
            "historyId": historyId,
            "limit": limit,
            "count": 0,
            "samples": [],
        }

    # Infer station/history from the first item if not provided
    station = stationName or items[0]["station_name"]
    hist_id = historyId or items[0]["history_id"]

    return {
        "stationName": station,
        "historyId": hist_id,
        "limit": limit,
        "count": len(items),
        "samples": items,
    }
@app.get("/debug/comfort")
def debug_zone_comfort(
    stationName: str = Query(..., description="Niagara stationName, e.g. AmsShop"),
    historyId: str = Query(..., description="History ID, e.g. /AmsShop/MaxSpaceTemp"),
    limit: int = Query(288, ge=10, le=2000, description="Max samples to use"),
    setpoint: float = Query(75.0, description="Temporary constant setpoint in °F"),
):
    """
    Compute a simple comfort score for a single history series using
    the in-memory MQTT history store.

    For now this assumes:
      - `value` from MQTT = zone temperature
      - A constant setpoint (query param) for all samples
    """
    samples = get_recent(
        station=stationName,
        history_id=historyId,
        limit=limit,
    )

    if not samples:
        return {
            "stationName": stationName,
            "historyId": historyId,
            "limit": limit,
            "setpoint_used_degF": setpoint,
            "analytics": {
                "samples": 0,
                "within_band_pct": None,
                "mean_error_degF": None,
            },
        }

    c = _config.comfort

    ts_col = c.timestamp_column
    t_col = c.temp_column
    sp_col = c.setpoint_column
    equip_col = c.equip_column

    # Build a DataFrame with the columns the comfort code expects
    df = pd.DataFrame(samples)

    # Map MQTT fields → configured columns
    df[ts_col] = pd.to_datetime(df["timestamp"])
    df[t_col] = df["value"]
    df[sp_col] = float(setpoint)
    df[equip_col] = historyId  # dummy equip label for now

    result = compute_zone_comfort(df, c)

    return {
        "stationName": stationName,
        "historyId": historyId,
        "limit": limit,
        "setpoint_used_degF": setpoint,
        "analytics": result,
    }
