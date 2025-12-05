# src/analytics/zone_pairs.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from ..store import sqlite_store
from ..niagara_client.mqtt_history_ingest import niagara_canonical_name
from .role_rules import infer_role


# ---------------------------------------------------------------------------
# DATACLASS: Equipment / Zone Pair
# ---------------------------------------------------------------------------


@dataclass
class ZonePair:
    station: str
    zone_root: str  # canonicalised equipment name
    equipment: Optional[str] = None
    floor: Optional[str] = None

    # History IDs for each analytic role
    space_temp: Optional[str] = None
    space_temp_sp: Optional[str] = None
    flow: Optional[str] = None
    flow_sp: Optional[str] = None
    damper: Optional[str] = None
    reheat: Optional[str] = None
    fan_cmd: Optional[str] = None
    fan_status: Optional[str] = None

    cooling_valve: Optional[str] = None
    heating_valve: Optional[str] = None
    compressor_cmd: Optional[str] = None
    compressor_status: Optional[str] = None

    # Human-friendly labels (from n:displayName)
    space_temp_name: Optional[str] = None
    space_temp_sp_name: Optional[str] = None
    flow_name: Optional[str] = None
    flow_sp_name: Optional[str] = None
    damper_name: Optional[str] = None
    reheat_name: Optional[str] = None
    fan_cmd_name: Optional[str] = None
    fan_status_name: Optional[str] = None
    cooling_valve_name: Optional[str] = None
    heating_valve_name: Optional[str] = None
    compressor_cmd_name: Optional[str] = None
    compressor_status_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _canonical_zone_root_from_equipment(equipment: str) -> str:
    """
    Convert a Niagara equipment label into a stable zone_root key
    using the same canonicalisation as MQTT history samples.
    """
    return niagara_canonical_name(equipment)


def _ensure_zone(
    index: Dict[str, Dict[str, ZonePair]],
    station: str,
    equipment: str,
    floor: Optional[str],
) -> ZonePair:
    """
    Get or create the ZonePair for (station, equipment).
    """
    zones_for_station = index.setdefault(station, {})

    zone_root = _canonical_zone_root_from_equipment(equipment)
    zone = zones_for_station.get(zone_root)
    if zone is None:
        zone = ZonePair(
            station=station,
            zone_root=zone_root,
            equipment=equipment,
            floor=floor,
        )
        zones_for_station[zone_root] = zone
    else:
        # Keep latest non-null floor / equipment if they appear later
        if zone.equipment is None:
            zone.equipment = equipment
        if zone.floor is None and floor is not None:
            zone.floor = floor

    return zone


# Map from role name -> (attr for history_id, attr for display label)
_ROLE_ATTR_MAP: Dict[str, tuple[str, str]] = {
    "space_temp": ("space_temp", "space_temp_name"),
    "space_temp_sp": ("space_temp_sp", "space_temp_sp_name"),
    "flow": ("flow", "flow_name"),
    "flow_sp": ("flow_sp", "flow_sp_name"),
    "damper": ("damper", "damper_name"),
    "reheat": ("reheat", "reheat_name"),
    "fan_cmd": ("fan_cmd", "fan_cmd_name"),
    "fan_status": ("fan_status", "fan_status_name"),
    "cooling_valve": ("cooling_valve", "cooling_valve_name"),
    "heating_valve": ("heating_valve", "heating_valve_name"),
    "compressor_cmd": ("compressor_cmd", "compressor_cmd_name"),
    "compressor_status": ("compressor_status", "compressor_status_name"),
}


# ---------------------------------------------------------------------------
# Core: build zone index from series metadata
# ---------------------------------------------------------------------------


def zone_pairs_as_dicts(limit: int = 50_000) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Build a nested index of zones/equipment and their analytic roles from the
    combined series metadata in sqlite_store.

        {
          "AmsShop": {
            "vav1_01": {
              "station": "AmsShop",
              "zone_root": "vav1_01",
              "equipment": "Vav1_01",
              "floor": "1",
              "space_temp": "/AmsShop/VAV$201$2d01_SpaceTemperature",
              "space_temp_name": "SpaceTemperature",
              "flow_sp": "/AmsShop/VAV$201$2d01_AirflowSetpoint",
              "flow_sp_name": "AirflowSetpoint",
              ...
            },
            ...
          },
          ...
        }

    This is what /debug/zone_pairs and zone_health use.
    """
    # Combined series + metadata from SQLite + in-memory overlay
    series = sqlite_store.list_series(limit=limit)

    index: Dict[str, Dict[str, ZonePair]] = {}

    for row in series:
        station = row.get("station")
        if not station:
            continue

        equipment = row.get("equipment")
        if not equipment:
            # No equipment means we can't attach this point to a zone
            continue

        history_id: str = row.get("history_id") or ""
        if not history_id:
            continue

        point_name: str = row.get("point_name") or history_id
        floor: Optional[str] = row.get("floor")
        tags_raw = row.get("tags") or []

        # Infer the analytic role using label + tags
        role = infer_role(point_name, tags_raw)
        if not role:
            # This point does not participate in analytics roles
            continue

        # Ensure there's a ZonePair for this (station, equipment)
        zone = _ensure_zone(index, station=station, equipment=equipment, floor=floor)

        # Map the role name to ZonePair attributes
        attrs = _ROLE_ATTR_MAP.get(role)
        if not attrs:
            # Unknown role string – ignore without failing
            continue

        hist_attr, name_attr = attrs

        # First writer wins: don't override if already set
        if getattr(zone, hist_attr) is None:
            setattr(zone, hist_attr, history_id)
            setattr(zone, name_attr, point_name)

    # Convert nested ZonePair objects into plain dicts
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for st_name, zones in index.items():
        out[st_name] = {}
        for z_root, zp in zones.items():
            out[st_name][z_root] = zp.to_dict()

    return out


# ---------------------------------------------------------------------------
# Helper to find a specific zone
# ---------------------------------------------------------------------------


def find_zone_pair(
    pairs_by_station: Dict[str, Dict[str, Dict[str, Any]]],
    station: str,
    zone_root: str,
) -> Optional[Dict[str, Any]]:
    """
    Look up a single zone pair dict from the nested index produced by
    zone_pairs_as_dicts().
    """
    zones = pairs_by_station.get(station)
    if not zones:
        return None
    return zones.get(zone_root)
