# src/niagara_client/mqtt_history_ingest.py

from __future__ import annotations

import re
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional, Any

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


def niagara_decode_name(name: str) -> str:
    """
    Decode Niagara-style hex escapes and insert spaces between words,
    covering camelCase as well as digit/letter transitions.

    Examples:
      "Zone$2d1$20Space$20Temp" -> "Zone-1 Space Temp"
      "maxSpaceTemp"            -> "max Space Temp"
      "VAV1$2d01"               -> "VAV 1-01"
      "Vav1_01"                 -> "Vav 1-01"
      "ZN1Temp"                 -> "ZN 1 Temp"
    """
    if not name:
        return name

    # Decode common Niagara hex escapes
    s = name.replace("$20", " ").replace("$2d", "-")

    # Treat underscores like dashes (engineer convenience)
    # Example: "Vav1_01" -> "Vav1-01"
    s = s.replace("_", "-")

    # CamelCase boundary: lowercase/digit -> uppercase
    # e.g. "maxSpaceTemp" -> "max Space Temp"
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)

    # Letter/digit boundaries in both directions
    # e.g. "VAV1-01" -> "VAV 1-01", "ZN1Temp" -> "ZN 1 Temp"
    s = re.sub(r"(?<=[A-Za-z])(?=[0-9])", " ", s)
    s = re.sub(r"(?<=[0-9])(?=[A-Za-z])", " ", s)

    # Normalize whitespace and repeated dashes
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"-{2,}", "-", s)

    return s


def niagara_canonical_name(name: str) -> str:
    """
    Turn a decoded Niagara label into a machine-safe, snake_case key.

    Example:
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


def _decode_history_label(raw_station: str, raw_history: str) -> str:
    """
    Build a human-readable history label from a Niagara history path,
    dropping a redundant root segment that matches stationName and
    preserving the remaining hierarchy.

    Examples:
      stationName="AmsShop",
      raw_history="/AmsShop/MaxSpaceTemp"
        -> "Max Space Temp"

      stationName="Regency Plaza",
      raw_history="/Regency Plaza/1st Floor/Vav1$2d01$20Space$20Temp"
        -> "1st Floor / VAV 1-01 Space Temp"
    """
    station_dec = niagara_decode_name(raw_station or "")
    history_dec = niagara_decode_name(raw_history or "")

    segments = [seg.strip() for seg in history_dec.split("/") if seg.strip()]

    if not segments:
        return history_dec.strip() or "missing_history_id"

    station_can = niagara_canonical_name(station_dec) or ""
    first_can = niagara_canonical_name(segments[0]) or ""

    if station_can and first_can and station_can == first_can:
        remaining = segments[1:]
        if remaining:
            segments = remaining

    if not segments:
        return history_dec.strip() or "missing_history_id"

    return " / ".join(segments)


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


def _validate_history_frame(data: Any) -> dict:
    """
    Basic JSON-schema-style validation for incoming MQTT history frames.

    Ensures the payload looks like:

        {
          "stationName": str,
          "messageType": "history",
          "historyId": str?,   # may be missing/empty; we will fallback
          "historyData": [
            { "timestamp": str, "status": str?, "value": number },
            ...
          ]
        }

    Returns the dict back if valid, otherwise raises ValueError.
    """
    if not isinstance(data, dict):
        raise ValueError("history frame must be a JSON object")

    if data.get("messageType") != "history":
        raise ValueError("messageType must be 'history'")

    station = data.get("stationName")
    rows = data.get("historyData")
    history_id = data.get("historyId", None)

    if not isinstance(station, str) or not station:
        raise ValueError("stationName must be a non-empty string")

    # historyId is allowed to be missing/empty; we handle fallback later.
    # If provided, enforce it is a string.
    if history_id is not None and not isinstance(history_id, str):
        raise ValueError("historyId must be a string when provided")

    if not isinstance(rows, list) or not rows:
        raise ValueError("historyData must be a non-empty list")

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"historyData[{idx}] must be an object")
        if "timestamp" not in row or "value" not in row:
            raise ValueError(f"historyData[{idx}] missing timestamp or value")
        if not isinstance(row["timestamp"], str):
            raise ValueError(f"historyData[{idx}].timestamp must be a string")
        # value can be int/float; we'll let float() raise if it can't convert

    return data


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
            print(
                f"[mqtt] connected to {broker_host}:{broker_port}, "
                f"subscribing to {topic}"
            )
            client.subscribe(topic)
        else:
            print(f"[mqtt] connection failed with code {rc}")

    def _on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        try:
            payload_str = msg.payload.decode("utf-8")
            data = json.loads(payload_str)

            # Ignore non-history messages quietly
            if not isinstance(data, dict) or data.get("messageType") != "history":
                return

            # Validate overall structure
            data = _validate_history_frame(data)

            raw_station = data.get("stationName", "")

            # Prefer explicit historyId, but fall back to other common fields like "id"
            raw_history = data.get("historyId", None)
            if not isinstance(raw_history, str) or not raw_history.strip():
                # Niagara is currently publishing "id": "/AmsShop/MaxSpaceTemp"
                alt = (
                    data.get("id")
                    or data.get("historyName")
                    or data.get("name")
                )
                if isinstance(alt, str) and alt.strip():
                    raw_history = alt
                else:
                    print("[mqtt] WARNING: missing historyId; using fallback 'missing_history_id'")
                    # Optional: log a truncated payload to help debugging
                    try:
                        print(f"[mqtt] offending payload (truncated): {json.dumps(data)[:300]}")
                    except Exception:
                        pass
                    raw_history = "missing_history_id"

            station_name = niagara_decode_name(raw_station)
            history_id = _decode_history_label(raw_station, raw_history)

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
