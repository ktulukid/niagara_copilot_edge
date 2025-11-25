from datetime import datetime, timedelta

from fastapi import FastAPI, Query
from pydantic import BaseModel

from ..config import load_config
from ..niagara_client.factory import make_history_client
from ..analytics.comfort import compute_zone_comfort

import os
from typing import List
from ..niagara_client.mqtt_history_ingest import (
    make_history_mqtt_client,
    HistorySample,
)


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
    This is where you integrate MQTT history into your storage layer.

    For now it just logs; you can later:
      - write to SQLite/Postgres
      - append to parquet
      - keep an in-memory cache keyed by (station_name, history_id)
    """
    if not samples:
        return
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