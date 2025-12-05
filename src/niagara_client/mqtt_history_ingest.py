from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import paho.mqtt.client as mqtt

from ..config import AppConfig, MqttConfig


# ---------------------------------------------------------------------------
# Name canonicalisation helpers
# ---------------------------------------------------------------------------


def niagara_decode_name(name: str) -> str:
    """
    Decode Niagara-ish escaped names into a human-friendly label.

    Example:
        "Vav1$2d01$20SpaceTemperature" -> "Vav1-01 Space Temperature"
    """
    if not isinstance(name, str):
        return str(name)

    # Replace common Niagara hex escapes
    s = name.replace("$20", " ").replace("$2d", "-")

    # Collapse multiple spaces
    while "  " in s:
        s = s.replace("  ", " ")

    return s.strip()


def niagara_canonical_name(name: str) -> str:
    """
    Convert a human-readable Niagara name into a stable snake_case key.

    This is used for station_key/history_key and must be deterministic.
    """
    if not isinstance(name, str):
        name = str(name)

    s = niagara_decode_name(name)
    out: List[str] = []
    prev_is_alnum = False

    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
            prev_is_alnum = True
        else:
            if prev_is_alnum and (not out or out[-1] != "_"):
                out.append("_")
            prev_is_alnum = False

    key = "".join(out).strip("_")
    return key or "unnamed"


# ---------------------------------------------------------------------------
# History sample model
# ---------------------------------------------------------------------------


@dataclass
class HistorySample:
    station_name: str
    history_id: str
    timestamp: datetime
    value: float
    status: Optional[str] = None

    # Extra metadata from MQTT payload
    equipment: Optional[str] = None
    floor: Optional[str] = None
    point_name: Optional[str] = None
    unit: Optional[str] = None
    tags: Optional[List[str]] = None

    @property
    def station_key(self) -> str:
        return niagara_canonical_name(self.station_name)

    @property
    def history_key(self) -> str:
        return niagara_canonical_name(self.history_id)


# ---------------------------------------------------------------------------
# MQTT frame validation / parsing (history)
# ---------------------------------------------------------------------------


def _parse_timestamp(ts: str) -> datetime:
    """
    Parse Niagara-style timestamps like '2025-11-29 00:30:00.249-0700'
    into timezone-aware datetimes.
    """
    fmt_variants = [
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
    ]
    last_error: Optional[Exception] = None
    for fmt in fmt_variants:
        try:
            return datetime.strptime(ts, fmt)
        except Exception as e:  # noqa: BLE001
            last_error = e
    raise ValueError(f"Unrecognised timestamp format: {ts!r}; last_error={last_error}")


def _extract_point_metadata(point_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull out fields we care about from the nested 'point' object.
    """
    if not isinstance(point_obj, dict):
        raise ValueError("'point' must be an object")

    # Prefer displayName if present, fall back to name
    point_name = point_obj.get("n:displayName") or point_obj.get("n:name")
    history_id = (
        point_obj.get("n:history")
        or point_obj.get("hs:history")
        or point_name
    )

    if not point_name:
        raise ValueError("point.n:name / point.n:displayName missing or empty")
    if not history_id:
        raise ValueError("point.n:history missing and no fallback name available")

    unit = (
        point_obj.get("hs:unit")
        or point_obj.get("h4:unit")
        or point_obj.get("n:units")
    )

    # Derive simple tag list from any *:tag == "Marker"
    tags: List[str] = []
    for key, val in point_obj.items():
        if val != "Marker":
            continue
        if not isinstance(key, str):
            continue
        # Accept m:, hs:, h4:, etc.
        if ":" in key:
            _, suffix = key.split(":", 1)
        else:
            suffix = key
        if suffix:
            tags.append(suffix)

    return {
        "point_name": str(point_name),
        "history_id": str(history_id),
        "unit": unit if unit is None else str(unit),
        "tags": tags or None,
    }


def _validate_history_frame(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and normalise a single MQTT JSON message for history.
    """
    if not isinstance(msg, dict):
        raise ValueError("MQTT payload must be a JSON object")

    message_type = msg.get("messageType")
    if message_type != "history":
        raise ValueError(f"Unsupported messageType={message_type!r}, expected 'history'")

    station_name = msg.get("stationName")
    if not station_name:
        raise ValueError("stationName is required")

    equipment = msg.get("equipment")
    if equipment is None:
        equipment = msg.get("metadataProperty")

    point_obj = msg.get("point")
    if point_obj is None:
        raise ValueError("point object is required in history frame")

    floor = msg.get("floor")
    if floor is None and isinstance(point_obj, dict):
        floor_num = point_obj.get("h4:floorNum")
        if floor_num is not None:
            floor = floor_num

    point_meta = _extract_point_metadata(point_obj)

    raw_rows = msg.get("historyData")
    if not isinstance(raw_rows, Sequence) or not raw_rows:
        raise ValueError("historyData must be a non-empty array")

    return {
        "station_name": str(station_name),
        "equipment": str(equipment) if equipment is not None else None,
        "floor": str(floor) if floor is not None else None,
        "point_name": point_meta["point_name"],
        "history_id": point_meta["history_id"],
        "unit": point_meta["unit"],
        "tags": point_meta["tags"],
        "rows": raw_rows,
    }


def decode_history_frame(msg: Dict[str, Any]) -> List[HistorySample]:
    """
    Convert a validated MQTT JSON history frame into a list of HistorySample.
    """
    data = _validate_history_frame(msg)

    station_name: str = data["station_name"]
    equipment: Optional[str] = data["equipment"]
    floor: Optional[str] = data["floor"]
    point_name: str = data["point_name"]
    history_id: str = data["history_id"]
    unit: Optional[str] = data["unit"]
    tags: Optional[List[str]] = data["tags"]
    rows: Sequence[Dict[str, Any]] = data["rows"]

    samples: List[HistorySample] = []

    for row in rows:
        ts_raw = row.get("timestamp")
        if ts_raw is None:
            continue

        try:
            ts = _parse_timestamp(str(ts_raw))
        except Exception:
            continue

        value = row.get("value")
        if value is None:
            continue

        try:
            val_float = float(value)
        except (TypeError, ValueError):
            continue

        status = row.get("status")
        if status is not None:
            status = str(status)

        samples.append(
            HistorySample(
                station_name=station_name,
                history_id=history_id,
                timestamp=ts,
                value=val_float,
                status=status,
                equipment=equipment,
                floor=floor,
                point_name=point_name,
                unit=unit,
                tags=list(tags) if tags is not None else None,
            )
        )

    return samples


# ---------------------------------------------------------------------------
# Equipment (zone) payload handler – stub for now
# ---------------------------------------------------------------------------


def _on_equipment_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    """
    Handle equipment / zone JSON published on mqtt_cfg.equipment_topic.

    For now, we just validate that it's JSON and log its basic shape.
    Later steps will parse this into a zones table.
    """
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
    except Exception as e:  # noqa: BLE001
        print(f"[mqtt] failed to decode equipment JSON payload: {e}")
        return

    if isinstance(data, list):
        print(f"[mqtt] received {len(data)} equipment records from topic {msg.topic}")
    elif isinstance(data, dict):
        print(f"[mqtt] received single equipment record from topic {msg.topic}")
    else:
        print(f"[mqtt] unexpected equipment payload type: {type(data)} on {msg.topic}")


# ---------------------------------------------------------------------------
# MQTT client wiring
# ---------------------------------------------------------------------------


def _on_mqtt_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    """
    Default MQTT callback: history frames on history_topic.
    """
    from ..store import history_store, sqlite_store

    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
    except Exception as e:  # noqa: BLE001
        print(f"[mqtt] failed to decode JSON payload: {e}")
        return

    all_samples: List[HistorySample] = []

    try:
        if isinstance(data, list):
            for idx, frame in enumerate(data):
                if not isinstance(frame, dict):
                    print(f"[mqtt] skipping non-object frame at index {idx}: {type(frame)}")
                    continue
                try:
                    all_samples.extend(decode_history_frame(frame))
                except Exception as e:  # noqa: BLE001
                    print(f"[mqtt] invalid history frame at index {idx}: {e}")
        elif isinstance(data, dict):
            all_samples = decode_history_frame(data)
        else:
            print(f"[mqtt] unexpected JSON root type: {type(data)}")
            return
    except Exception as e:  # extra safety
        print(f"[mqtt] invalid history frame: {e}")
        return

    if not all_samples:
        return

    try:
        history_store.add_batch(all_samples)
    except Exception as e:  # noqa: BLE001
        print(f"[mqtt] failed to add to in-memory history_store: {e}")

    try:
        sqlite_store.add_batch(all_samples)
    except Exception as e:  # noqa: BLE001
        print(f"[mqtt] failed to add to sqlite_store: {e}")


def make_history_mqtt_client(cfg: AppConfig) -> mqtt.Client:
    """
    Create and connect an MQTT client that listens for:
      - history frames on mqtt.history_topic
      - equipment/zone payloads on mqtt.equipment_topic
    """
    mqtt_cfg: MqttConfig = cfg.mqtt

    client = mqtt.Client()

    # Optional authentication
    if mqtt_cfg.username:
        password = None
        if mqtt_cfg.password_env:
            import os

            password = os.getenv(mqtt_cfg.password_env) or None
        client.username_pw_set(mqtt_cfg.username, password=password)

    # History frames use the default on_message
    client.on_message = _on_mqtt_message

    client.connect(mqtt_cfg.host, mqtt_cfg.port, keepalive=60)

    # Subscribe to both topics
    client.subscribe(mqtt_cfg.history_topic)
    client.subscribe(mqtt_cfg.equipment_topic)

    # Attach dedicated handler for equipment topic
    client.message_callback_add(mqtt_cfg.equipment_topic, _on_equipment_message)

    client.loop_start()
    print(
        f"[mqtt] subscribed to history='{mqtt_cfg.history_topic}' "
        f"and equipment='{mqtt_cfg.equipment_topic}' on "
        f"{mqtt_cfg.host}:{mqtt_cfg.port}"
    )
    return client
