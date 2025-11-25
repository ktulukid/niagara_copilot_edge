# src/niagara_client/mqtt_history_ingest.py

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

import paho.mqtt.client as mqtt


@dataclass
class HistorySample:
    station_name: str
    history_id: str
    timestamp: datetime
    status: str
    value: float


# Type of callback your app will provide
BatchHandler = Callable[[List[HistorySample]], None]


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

            station_name = data["stationName"]
            history_id = data["historyId"]
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
