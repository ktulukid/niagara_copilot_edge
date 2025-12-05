from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel


# ---------------------------------------
# Data Source Types (simplified)
# ---------------------------------------

DataSourceType = Literal[
    "mqtt_history",
    "haystack",
]


class MqttConfig(BaseModel):
    host: str = "localhost"
    port: int = 1883
    history_topic: str = "niagara/histories"
    equipment_topic: str = "niagara/equipment"  # NEW

    # Optional auth
    username: Optional[str] = None
    password_env: Optional[str] = None


class HaystackConfig(BaseModel):
    uri: str
    username: str
    password_env: str = "NIAGARA_PASSWORD"
    project: str = "default"


class DataSourceConfig(BaseModel):
    type: DataSourceType

    # mqtt_history uses AppConfig.mqtt
    haystack: Optional[HaystackConfig] = None


class ComfortConfig(BaseModel):
    occupied_start: str          # "07:00"
    occupied_end: str            # "18:00"
    setpoint_column: str
    temp_column: str
    timestamp_column: str
    equip_column: str
    comfort_band_degF: float


class AppConfig(BaseModel):
    site_name: str
    data_source: DataSourceConfig
    comfort: ComfortConfig
    mqtt: MqttConfig = MqttConfig()

    # Local SQLite history store path and retention
    db_path: str = "data/history.sqlite"
    db_retention_hours: int = 24 * 30  # 30 days default

    # Optional global Haystack defaults
    haystack: Optional[HaystackConfig] = None


def load_config(path: Path | str = "config/config.yaml") -> AppConfig:
    """
    Load YAML config, then (optionally) prompt for MQTT host/port overrides.

    If stdin is not available (e.g. non-interactive run), it will silently
    skip prompts and just use values from config.yaml / defaults.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cfg = AppConfig.parse_obj(raw)

    # Interactive override of MQTT host/port (restored behaviour)
    try:
        default_host = cfg.mqtt.host
        default_port = cfg.mqtt.port

        host_in = input(f"Enter MQTT broker address [{default_host}]: ").strip()
        port_in = input(f"Enter MQTT broker port [{default_port}]: ").strip()

        if host_in:
            cfg.mqtt.host = host_in

        if port_in:
            try:
                cfg.mqtt.port = int(port_in)
            except ValueError:
                print(f"Invalid port '{port_in}', keeping {default_port}")

    except EOFError:
        # Non-interactive environment: keep YAML/default values
        pass

    return cfg
