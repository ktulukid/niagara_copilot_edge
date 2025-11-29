from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Protocol, Any

from datetime import datetime

from ..config import AppConfig, HaystackConfig as CfgHaystackConfig
from .haystack_client import (
    HaystackHistoryClient,
    HaystackConfig as HSClientConfig,
)


# -----------------------------------------------------------------------------
# History client protocol
# -----------------------------------------------------------------------------


class HistoryClient(Protocol):
    """
    Minimal protocol for a history/metadata client.

    This is intentionally aligned with the Haystack client we’ve defined so far:
      - read_by_filter(filter_expr: str, limit: int = 1000) -> list[dict[str, Any]]
      - his_read(entity_id: str, range_str: str)
          -> list[tuple[datetime, float]]

    Other concrete clients (CSV export, MQTT-backed, etc.) can implement a
    compatible surface later if you decide to route everything through this
    abstraction.
    """

    def read_by_filter(self, filter_expr: str, limit: int = 1000) -> list[dict[str, Any]]:
        ...

    def his_read(self, entity_id: str, range_str: str) -> list[tuple[datetime, float]]:
        ...


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------


@dataclass
class _ResolvedHaystackConfig:
    uri: str
    username: str
    password: str
    project: str


def _resolve_haystack_config(app_config: AppConfig) -> Optional[_ResolvedHaystackConfig]:
    """
    Resolve Haystack connection parameters from AppConfig.

    Precedence:
      1. app_config.haystack (if present)
      2. app_config.data_source.haystack (if present)

    Returns None if no Haystack config is defined.
    """
    hs_cfg: Optional[CfgHaystackConfig] = None

    if getattr(app_config, "haystack", None) is not None:
        hs_cfg = app_config.haystack
    elif getattr(app_config.data_source, "haystack", None) is not None:
        hs_cfg = app_config.data_source.haystack

    if hs_cfg is None:
        return None

    password = os.getenv(hs_cfg.password_env, "")
    if not password:
        # We don't raise here; the caller can decide whether to treat this as fatal.
        print(
            f"[warn] Haystack password env '{hs_cfg.password_env}' is empty or not set"
        )  # noqa: T201

    return _ResolvedHaystackConfig(
        uri=hs_cfg.uri,
        username=hs_cfg.username,
        password=password,
        project=hs_cfg.project,
    )


# -----------------------------------------------------------------------------
# Public factories
# -----------------------------------------------------------------------------


def make_haystack_client(app_config: AppConfig) -> HaystackHistoryClient:
    """
    Construct a HaystackHistoryClient from AppConfig.

    This does NOT look at data_source.type; it just tries to resolve Haystack
    configuration from AppConfig (top-level or data_source.haystack).
    """
    resolved = _resolve_haystack_config(app_config)
    if resolved is None:
        raise ValueError(
            "Haystack configuration not found on AppConfig. "
            "Define either `haystack` or `data_source.haystack` in the YAML."
        )

    client = HaystackHistoryClient(
        HSClientConfig(
            uri=resolved.uri,
            username=resolved.username,
            password=resolved.password,
            proj=resolved.project,
        )
    )
    return client


def make_history_client(app_config: AppConfig) -> HistoryClient:
    """
    Unified history client factory, routed by app_config.data_source.type.

    Currently implemented:
      - type == "haystack"  -> HaystackHistoryClient

    Placeholders (raise NotImplementedError):
      - type == "niagara_csv_export"
      - type == "mqtt_json_stream"

    This keeps behavior explicit and safe until we wire the other paths to
    concrete implementations that match your existing repo structure.
    """
    ds_type = app_config.data_source.type

    if ds_type == "haystack":
        return make_haystack_client(app_config)

    if ds_type == "niagara_csv_export":
        raise NotImplementedError(
            "make_history_client for type 'niagara_csv_export' is not wired yet in "
            "this factory. Either switch to type 'haystack' or extend this "
            "function with your existing CSV history client implementation."
        )

    if ds_type == "mqtt_json_stream":
        raise NotImplementedError(
            "make_history_client for type 'mqtt_json_stream' is not wired yet in "
            "this factory. History for MQTT is currently handled via the "
            "mqtt_history_ingest pipeline and sqlite_store."
        )

    raise ValueError(f"Unsupported data_source.type: {ds_type!r}")
