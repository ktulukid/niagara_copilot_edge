# src/niagara_client/mqtt_history_ingest.py

import re
import json
import paho.mqtt.client as mqtt
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional
from __future__ import annotations



@dataclass
class HistorySample:
    station_name: str
    history_id: str
    timestamp: datetime
    status: str
    value: float


# Type of callback your app will provide
BatchHandler = Callable[[List[HistorySample]], None]

def niagara_decode_name(name: str) -> str:
    """
    Decode Niagara-style hex escapes into a human label.

    Examples:
      "Zone$2d1$20Space$20Temp" -> "Zone-1 Space Temp"
    """
    if not name:
        return name

    s = name
    s = s.replace("$20", " ")
    s = s.replace("$2d", "-")
    # You can extend for other codes later, e.g. "$2e" -> ".", etc.
    return s


def niagara_canonical_name(name: str) -> str:
    """
    Turn a decoded Niagara label into a machine-safe, snake_case key.

    Examples:
      "Zone-1 Space Temp" -> "zone_1_space_temp"
    """
    if not name:
        return name

    # First ensure it's decoded (idempotent if already decoded)
    decoded = niagara_decode_name(name)

    # Lowercase
    s = decoded.lower()

    # Replace any sequence of non-alphanumeric characters with "_"
    s = re.sub(r"[^a-z0-9]+", "_", s)

    # Trim leading/trailing underscores
    s = s.strip("_")

    return s


def _parse_timestamp(ts: str) -> datetime:
    """
    Niagara history timestamp format example:
        2025-11-24 00:00:01.349-0700
    """
    # with fractional seconds
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f%z")
    except ValueError:
        # fallback without fractional seconds
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S%z")


def make_history_mqtt_client(
    broker_host: str,
    broker_port: int = 1883,
    topic: str = "niagara/histories",
    on_batch: Optional[BatchHandler] = None,
) -> mqtt.Client:
    """
    Create an MQTT client subscribed to the given topic that parses
    Niagara history JSON messages of the form:

        {
          "stationName": "AmsShop",
          "messageType": "history",
          "historyId": "/AmsShop/MaxSpaceTemp",
          "historyData": [
            { "timestamp": "...", "status": "{ok}", "value": 77.58 },
            ...
          ]
        }

    For each MQTT message, `on_batch(samples)` is called with a list[HistorySample].
    """

    # default handler if you don't pass one in
    if on_batch is None:
        def on_batch(samples: List[HistorySample]) -> None:
            if not samples:
                print("[mqtt] received empty history batch")
                return
            first = samples[0]
            print(
                f"[mqtt] {len(samples)} samples for "
                f"{first.station_name} {first.history_id}"
            )

    client = mqtt.Client()

    def _on_connect(client: mqtt.Client, userdata, flags, rc):
        if rc == 0:
            print(f"[mqtt] connected to {broker_host}:{broker_port}, subscribing to {topic}")
            client.subscribe(topic)
        else:
            print(f"[mqtt] connection failed with code {rc}")

    def _on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        try:
            payload_str = msg.payload.decode("utf-8")
            data = json.loads(payload_str)

            if data.get("messageType") != "history":
                # ignore other message types for now
                return

            raw_station = payload.get("stationName", "")
            raw_history = payload.get("historyId", "")
            station_name = niagara_canonical_name(raw_station)
            history_id = niagara_canonical_name(raw_history)
            rows = data.get("historyData", [])

            samples: List[HistorySample] = []
            for row in rows:
                ts = _parse_timestamp(row["timestamp"])
                samples.append(
                    HistorySample(
                        station_name=station_name,
                        history_id=history_id,
                        timestamp=ts,
                        status=row.get("status", ""),
                        value=float(row["value"]),
                    )
                )

            if samples:
                on_batch(samples)

        except Exception as exc:
            # keep this noisy for now; we can change to proper logging later
            print(f"[mqtt] error parsing history message: {exc}")

    client.on_connect = _on_connect
    client.on_message = _on_message

    client.connect(broker_host, broker_port, keepalive=60)
    return client
