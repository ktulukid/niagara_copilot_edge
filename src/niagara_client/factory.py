# src/niagara_client/factory.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Any, Dict, List, Optional
import os

from ..config import AppConfig, HaystackConfig as CfgHaystackConfig
from .haystack_client import (
    HaystackHistoryClient,
    HaystackConfig as HSClientConfig,
)


class HistoryClient(Protocol):
    """Minimal interface for any history backend."""

    def read_by_filter(self, filter_expr: str, limit: int = 1000) -> List[Dict[str, Any]]:
        ...

    def his_read(self, entity_id: str, range_str: str) -> List[Any]:
        ...


@dataclass
class _ResolvedHaystackConfig:
    uri: str
    username: str
    password: str
    project: str


def _resolve_haystack_config(cfg: AppConfig) -> Optional[_ResolvedHaystackConfig]:
    hs_cfg: Optional[CfgHaystackConfig] = None

    if cfg.haystack is not None:
        hs_cfg = cfg.haystack
    elif cfg.data_source.haystack is not None:
        hs_cfg = cfg.data_source.haystack

    if hs_cfg is None:
        return None

    password = os.getenv(hs_cfg.password_env, "")
    if not password:
        print(f"[warn] Haystack password env '{hs_cfg.password_env}' is empty")

    return _ResolvedHaystackConfig(
        uri=hs_cfg.uri,
        username=hs_cfg.username,
        password=password,
        project=hs_cfg.project,
    )


def make_history_client(cfg: AppConfig) -> Optional[HistoryClient]:
    """
    Factory selection:
        - mqtt_history → None (ingestion already handled via MQTT)
        - haystack → HaystackHistoryClient
    """
    ds_type = cfg.data_source.type

    if ds_type == "mqtt_history":
        # No pull client needed — ingestion happens via MQTT ingest pipeline
        return None

    if ds_type == "haystack":
        resolved = _resolve_haystack_config(cfg)
        if resolved is None:
            raise ValueError("Haystack config missing.")

        return HaystackHistoryClient(
            HSClientConfig(
                uri=resolved.uri,
                username=resolved.username,
                password=resolved.password,
                proj=resolved.project,
            )
        )

    raise ValueError(f"Unsupported data_source.type: {ds_type!r}")
