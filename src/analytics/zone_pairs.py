# src/analytics/zone_pairs.py

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

from ..store import sqlite_store

# ---------------------------------------------------------------------------
# ZONE ROOT DETECTION
# ---------------------------------------------------------------------------
# We now support the following equipment root types:
#   VAV, FPB, RTU, AHU, FCU, EF/EFU, Boiler, Chiller
#
# Example valid matches:
#   "RTU 1", "Ahu-2", "FCU_03", "EF 3-01", "Chiller 1", "Boiler-2"
#   "VAV 1-13", "Fpb 3-01"
#
# Each must be followed by at least one number. A second number (for "-01")
# is optional, because many equipment (RTU, AHU) don't have a dash index.
# ---------------------------------------------------------------------------

ZONE_RE = re.compile(
    r"((?:vav|fpb|rtu|ahu|fcu|ef|efu|boiler|chiller)[\s_-]*\d+(?:[\s_-]*\d+)?)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# ROLE DETECTION
# ---------------------------------------------------------------------------
ROLE_PATTERNS = {
    # ---------------------------
    # Temperature + Setpoint
    # ---------------------------
    "space_temp": [
        r"space\s*temp",
        r"zone\s*temp",
        r"\bzn\s*t\b",
        r"room\s*temp",
        r"effective\s*space\s*temp",
        r"discharge\s*air\s*temp",          # for RTUs, AHUs, FCUs
        r"mixed\s*air\s*temp",
        r"return\s*air\s*temp",
        r"supply\s*air\s*temp",
    ],
    "space_temp_sp": [
        r"effective\s*setpoint",
        r"space\s*temp\s*sp",
        r"zone\s*setpoint",
        r"\bzn\s*sp\b",
        r"supply\s*air\s*temp\s*sp",
        r"rat\s*sp",                        # return air temp sp
        r"mat\s*sp",                        # mixed air temp sp
    ],

    # ---------------------------
    # Flow + Setpoint
    # ---------------------------
    "flow": [
        r"box\s*flow",
        r"air\s*flow",
        r"airflow",
        r"\bcfm\b(?!\s*sp)",
        r"supply\s*cfm",
        r"return\s*cfm",
        r"exhaust\s*cfm",
    ],
    "flow_sp": [
        r"flow\s*setpoint",
        r"\bcfm\s*sp\b",
        r"airflow\s*sp",
        r"min\s*cfm",
        r"max\s*cfm",
    ],

    # ---------------------------
    # Damper
    # ---------------------------
    "damper": [
        r"damper\s*position",
        r"damper\s*output",
        r"damper\s*cmd",
        r"\bdamper\b",
        r"oa\s*damper",   # AHU outside air damper
        r"ra\s*damper",   # return air damper
        r"ea\s*damper",   # exhaust
    ],

    # ---------------------------
    # Heating valve / Reheat
    # ---------------------------
    "reheat": [
        r"\breheat\b",
        r"rh\s*valve",
        r"heating\s*valve",
        r"hot\s*water\s*valve",
        r"boiler\s*valve",
    ],

    # ---------------------------
    # Fan Command / Status
    # ---------------------------
    "fan_cmd": [
        r"fan\s*command",
        r"fan\s*cmd",
        r"fan\s*enable",
        r"fan\s*enable\s*output",
        r"\bfan\s*ss\b",
        r"exhaust\s*fan\s*cmd",
        r"supply\s*fan\s*cmd",
        r"return\s*fan\s*cmd",
    ],
    "fan_status": [
        r"fan\s*status",
        r"fan\s*proof",
        r"supply\s*fan\s*status",
        r"return\s*fan\s*status",
        r"exhaust\s*fan\s*status",
    ],

    # ---------------------------
    # Cooling / Chiller control
    # ---------------------------
    "cooling_valve": [
        r"cooling\s*valve",
        r"chilled\s*water\s*valve",
        r"cw\s*valve",
    ],

    # ---------------------------
    # Heating / Boiler
    # ---------------------------
    "heating_valve": [
        r"heating\s*valve",
        r"hw\s*valve",
        r"boiler\s*valve",
    ],

    # ---------------------------
    # Compressors / Stages (RTU, Chiller)
    # ---------------------------
    "compressor_cmd": [
        r"compressor\s*\d*\s*cmd",
        r"stage\s*\d*\s*cmd",
    ],
    "compressor_status": [
        r"compressor\s*\d*\s*status",
        r"stage\s*\d*\s*status",
    ],
}

# ---------------------------------------------------------------------------
# DATACLASS: Equipment / Zone Pair
# ---------------------------------------------------------------------------

@dataclass
class ZonePair:
    station_key: str
    station_name: str
    zone_root: str  # e.g. "vav-1-01", "rtu-1", "ef-3-01"

    # Core analytics fields
    space_temp: Optional[str] = None
    space_temp_sp: Optional[str] = None
    flow: Optional[str] = None
    flow_sp: Optional[str] = None
    damper: Optional[str] = None
    reheat: Optional[str] = None

    # Fan command + status
    fan_cmd: Optional[str] = None
    fan_status: Optional[str] = None

    # RTU / AHU / Chiller / Boiler signals
    cooling_valve: Optional[str] = None
    heating_valve: Optional[str] = None
    compressor_cmd: Optional[str] = None
    compressor_status: Optional[str] = None


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _infer_zone_root(label: str) -> Optional[str]:
    if not label:
        return None
    s = label.lower()
    m = ZONE_RE.search(s)
    if not m:
        return None

    root = m.group(1)
    root = root.replace("_", "-").replace(" ", "-")
    root = re.sub(r"-{2,}", "-", root)
    return root.strip("-")


def _infer_role(label: str) -> Optional[str]:
    if not label:
        return None

    s = label.lower()
    for role, patterns in ROLE_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, s):
                return role
    return None


# ---------------------------------------------------------------------------
# MAIN INDEX BUILD
# ---------------------------------------------------------------------------

def build_zone_pair_index(limit: int = 5000) -> Dict[Tuple[str, str], ZonePair]:
    series = sqlite_store.list_series(limit=limit)
    index: Dict[Tuple[str, str], ZonePair] = {}

    for row in series:
        station_key = row.get("station_key") or ""
        station_name = row.get("stationName") or ""
        history_id = row.get("historyId") or ""

        zone_root = _infer_zone_root(history_id)
        if not zone_root:
            continue

        role = _infer_role(history_id)
        if not role:
            continue

        key = (station_key, zone_root)

        zp = index.get(key)
        if zp is None:
            zp = ZonePair(
                station_key=station_key,
                station_name=station_name,
                zone_root=zone_root,
            )
            index[key] = zp

        # Fill only the first discovered role
        if getattr(zp, role) is None:
            setattr(zp, role, history_id)

    return index


def find_zone_pair(station_key: str, zone_root: str, limit: int = 5000) -> Optional[ZonePair]:
    index = build_zone_pair_index(limit=limit)
    return index.get((station_key, zone_root))


def zone_pairs_as_dicts(limit: int = 5000) -> List[dict]:
    index = build_zone_pair_index(limit=limit)
    return [asdict(zp) for zp in index.values()]
