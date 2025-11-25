from pathlib import Path
from typing import Optional, Literal

from pydantic import BaseModel
import yaml


DataSourceType = Literal["niagara_csv_export", "mqtt_json_stream"]

class MqttConfig(BaseModel):
    host: str = "localhost"
    port: int = 1883
    history_topic: str = "niagara/histories"


class NiagaraCsvExportConfig(BaseModel):
    host: str                    # "172.20.40.22"
    ord_path: str                # "file:%5EhistoryExports/AmsShop"
    username: str
    password_env: str = "NIAGARA_PASSWORD"
    insecure_tls: bool = True


class MqttJsonStreamConfig(BaseModel):
    host: str
    port: int = 8883
    topic: str
    username: Optional[str] = None
    password_env: Optional[str] = None
    tls: bool = True
    client_id: Optional[str] = None
    keepalive: int = 60
    retention_hours: int = 24


class DataSourceConfig(BaseModel):
    type: DataSourceType
    niagara_csv_export: Optional[NiagaraCsvExportConfig] = None
    mqtt_json_stream: Optional[MqttJsonStreamConfig] = None


class ComfortConfig(BaseModel):
    occupied_start: str          # "07:00"
    occupied_end: str            # "18:00"
    setpoint_column: str
    temp_column: str
    equip_column: str
    timestamp_column: str
    comfort_band_degF: float


class AppConfig(BaseModel):
    site_name: str
    data_source: DataSourceConfig
    comfort: ComfortConfig
    mqtt: MqttConfig = MqttConfig()


def load_config(path: Path | str = "config/config.yaml") -> AppConfig:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return AppConfig.parse_obj(raw)
