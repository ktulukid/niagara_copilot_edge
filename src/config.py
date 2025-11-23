from pathlib import Path
from typing import Optional

from pydantic import BaseModel
import yaml


class DataSourceConfig(BaseModel):
    type: str  # "csv" for now
    path: Optional[str] = None


class ComfortConfig(BaseModel):
    occupied_start: str
    occupied_end: str
    setpoint_column: str
    temp_column: str
    equip_column: str
    timestamp_column: str
    comfort_band_degF: float


class AppConfig(BaseModel):
    site_name: str
    data_source: DataSourceConfig
    comfort: ComfortConfig


def load_config(config_path: str = "config/config.yaml") -> AppConfig:
    """
    Load YAML config from config/config.yaml and return an AppConfig object.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return AppConfig(**raw)
