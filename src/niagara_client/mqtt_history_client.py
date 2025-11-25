from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List

import pandas as pd
import paho.mqtt.client as mqtt

from ..config import AppConfig, MqttJsonStreamConfig

logger = logging.getLogger(__name__)


class MqttHistoryClient:
    """
    History client that subscribes to a JSON MQTT topic and keeps
    the most recent messages in memory for downstream filtering.
    """

    def __init__(self, app_config: AppConfig) -> None:
        ds = app_config.data_source
        if ds.type != "mqtt_json_stream":
            raise ValueError("MqttHistoryClient requires data_source.type == 'mqtt_json_stream'")

        mqtt_cfg = ds.mqtt_json_stream
        if mqtt_cfg is None:
            raise ValueError("mqtt_json_stream configuration is required for MQTT history client")

        self._cfg = app_config
        self._mqtt_cfg = mqtt_cfg
        self._records: List[Dict[str, Any]] = []
        self._lock = Lock()

        client_id = mqtt_cfg.client_id
        self._client = mqtt.Client(client_id=client_id) if client_id else mqtt.Client()

        if mqtt_cfg.username:
            password_env = mqtt_cfg.password_env
            if not password_env:
                raise ValueError("password_env must be provided when MQTT username is set")
            password = os.getenv(password_env)
            if not password:
                raise RuntimeError(
                    f"Environment variable {password_env} is not set for MQTT authentication."
                )
            self._client.username_pw_set(mqtt_cfg.username, password)

        if mqtt_cfg.tls:
            self._client.tls_set()

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

        self._client.connect(mqtt_cfg.host, port=mqtt_cfg.port, keepalive=mqtt_cfg.keepalive)
        self._client.loop_start()

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int) -> None:
        if rc != 0:
            logger.warning("MQTT connection returned non-zero result code %s", rc)
        client.subscribe(self._mqtt_cfg.topic)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        payload_text = msg.payload.decode("utf-8", errors="ignore")
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            logger.debug("Received invalid JSON payload on %s: %s", msg.topic, payload_text[:200])
            return

        ts_col = self._cfg.comfort.timestamp_column
        raw_ts = payload.get(ts_col)
        if raw_ts is None:
            logger.debug("MQTT payload missing timestamp column %s", ts_col)
            return

        try:
            parsed_ts = pd.to_datetime(raw_ts, utc=True).to_pydatetime()
            parsed_ts = parsed_ts.replace(tzinfo=None)
        except (ValueError, TypeError):
            logger.warning("Unable to parse timestamp %r from MQTT payload", raw_ts)
            return

        record = payload.copy()
        record["_parsed_timestamp"] = parsed_ts

        with self._lock:
            self._records.append(record)
            self._prune_records_locked()

    def _prune_records_locked(self) -> None:
        retention = timedelta(hours=self._mqtt_cfg.retention_hours)
        cutoff = datetime.utcnow() - retention
        self._records = [
            record for record in self._records
            if record.get("_parsed_timestamp") and record["_parsed_timestamp"] >= cutoff
        ]

    def get_zone_history(self, equip: str, start: datetime, end: datetime) -> pd.DataFrame:
        c = self._cfg.comfort
        ts_col = c.timestamp_column
        equip_col = c.equip_column

        with self._lock:
            filtered = []
            for record in self._records:
                ts = record.get("_parsed_timestamp")
                if ts is None or ts < start or ts > end:
                    continue
                if record.get(equip_col) != equip:
                    continue
                copy_record = record.copy()
                copy_record[ts_col] = ts
                filtered.append(copy_record)

        if not filtered:
            return pd.DataFrame(columns=[ts_col, equip_col])

        df = pd.DataFrame(filtered)
        df.drop(columns="_parsed_timestamp", inplace=True, errors="ignore")
        return df
