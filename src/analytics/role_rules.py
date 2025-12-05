# src/analytics/role_rules.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


# ---------------------------------------------------------------------------
# Role rule model
# ---------------------------------------------------------------------------


@dataclass
class RoleRule:
    role: str
    name_regex: List[str]
    tags_all: List[str]
    tags_any: List[str]
    priority: int = 100  # lower = stronger

    def matches(self, label: str, tags: Optional[Set[str]]) -> bool:
        """
        Check whether this rule matches a given label + tag set.

        Callers pass tags as a set of lowercase suffixes (e.g. "zone", "sp",
        "zoneairtempsensor"). We normalise again here for safety.
        """
        s = (label or "").lower()

        # Normalise incoming tags to a lowercase set
        tag_set: Set[str] = set()
        if tags:
            tag_set = {str(t).lower() for t in tags}

        # tags_all: all required tags must be present
        if self.tags_all:
            required = {str(t).lower() for t in self.tags_all}
            if not required.issubset(tag_set):
                return False

        # tags_any: at least one tag must be present (if specified)
        if self.tags_any:
            any_required = {str(t).lower() for t in self.tags_any}
            if not (tag_set & any_required):
                return False

        # name_regex: at least one must match (if specified)
        if self.name_regex:
            return any(re.search(pat, s) for pat in self.name_regex)

        # If no name_regex, tags were enough
        return True


# ---------------------------------------------------------------------------
# Default rules – tag-first, name-fallback
#
# These are written against the suffix tags produced by mqtt_history_ingest:
#   - m:zoneAirTempSensor      -> "zoneairtempsensor"
#   - m:air                    -> "air"
#   - m:flow                   -> "flow"
#   - m:zone                   -> "zone"
#   - m:sp                     -> "sp"
#   - m:sensor                 -> "sensor"
#
# History ingestion flattens *:X == "Marker" into tag list ["x", ...].
# ---------------------------------------------------------------------------


_DEFAULT_RULES_SPEC: Dict[str, Any] = {
    "rules": [
        # ---------------------------
        # Temperature + Setpoint
        # ---------------------------
        # Strong tag-based: dedicated zone air temp sensor tag
        {
            "role": "space_temp",
            "priority": 4,
            "name_regex": [],
            "tags_all": ["zoneairtempsensor"],
            "tags_any": [],
        },
        # Tag combo: zone + temp + sensor
        {
            "role": "space_temp",
            "priority": 5,
            "name_regex": [],
            "tags_all": ["zone", "temp", "sensor"],
            "tags_any": [],
        },
        # Name-based fallback
        {
            "role": "space_temp",
            "priority": 10,
            "name_regex": [
                r"space\s*temp",
                r"zone\s*temp",
                r"\bzn\s*t\b",
                r"room\s*temp",
                r"effective\s*space\s*temp",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # Space temp setpoint – tag-based
        {
            "role": "space_temp_sp",
            "priority": 4,
            "name_regex": [],
            "tags_all": ["zone", "temp", "sp"],
            "tags_any": [],
        },
        # Name-based fallback for effective setpoints, etc.
        {
            "role": "space_temp_sp",
            "priority": 10,
            "name_regex": [
                r"space\s*temp\s*setpoint",
                r"zone\s*temp\s*setpoint",
                r"room\s*temp\s*setpoint",
                r"effective\s*setpoint",
                r"\bzn\s*sp\b",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # ---------------------------
        # Flow + Setpoint
        # ---------------------------
        # Box/zone airflow – tag-driven
        {
            "role": "flow",
            "priority": 6,
            "name_regex": [],
            "tags_all": ["air", "flow", "zone"],
            "tags_any": [],
        },
        # Name-based fallback
        {
            "role": "flow",
            "priority": 10,
            "name_regex": [
                r"box\s*flow",
                r"air\s*flow",
                r"airflow",
                r"\bcfm\b(?!\s*sp)",
                r"supply\s*cfm",
                r"return\s*cfm",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # Flow setpoint – tag-driven
        # NOTE: lower priority number than "flow" so it wins when "sp" is present
        {
            "role": "flow_sp",
            "priority": 4,
            "name_regex": [],
            "tags_all": ["air", "flow", "zone", "sp"],
            "tags_any": [],
        },
        # Name-based fallback
        {
            "role": "flow_sp",
            "priority": 10,
            "name_regex": [
                r"flow\s*setpoint",
                r"\bcfm\s*sp\b",
                r"airflow\s*sp",
                r"min\s*cfm",
                r"max\s*cfm",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # ---------------------------
        # Damper
        # ---------------------------
        # Damper position – tag-driven (if you add an m:damper tag)
        {
            "role": "damper",
            "priority": 5,
            "name_regex": [],
            "tags_all": ["damper"],
            "tags_any": [],
        },
        # Name fallback
        {
            "role": "damper",
            "priority": 10,
            "name_regex": [
                r"damper\s*position",
                r"damper\s*output",
                r"oa\s*damper",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # ---------------------------
        # Reheat
        # ---------------------------
        # Reheat valve / output – tag-driven if you tag with "reheat"
        {
            "role": "reheat",
            "priority": 5,
            "name_regex": [],
            "tags_all": ["reheat"],
            "tags_any": [],
        },
        # Name-based fallback
        {
            "role": "reheat",
            "priority": 10,
            "name_regex": [
                r"\breheat\b",
                r"rh\s*valve",
                r"heating\s*valve",
                r"hot\s*water\s*valve",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # ---------------------------
        # Fan command / status
        # ---------------------------
        # Fan command – tag-driven (if you introduce tags like "fan_cmd")
        {
            "role": "fan_cmd",
            "priority": 5,
            "name_regex": [],
            "tags_all": [],
            "tags_any": ["fan_cmd", "fancommand", "fancommanded"],
        },
        # Special-case: use "Supply Fan Speed" as fan_cmd for VFD units
        {
            "role": "fan_cmd",
            "priority": 6,
            "name_regex": [
                r"supply\s*fan\s*speed",
            ],
            "tags_all": [],
            "tags_any": [],
        },
        # Legacy name-based command
        {
            "role": "fan_cmd",
            "priority": 10,
            "name_regex": [
                r"fan\s*cmd",
                r"fan\s*command",
                r"supply\s*fan\s*cmd",
                r"supply\s*fan\s*command",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # Fan status / proof
        {
            "role": "fan_status",
            "priority": 10,
            "name_regex": [
                r"fan\s*status",
                r"fan\s*proof",
                r"supply\s*fan\s*status",
                r"supply\s*fan\s*proof",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # ---------------------------
        # Cooling / Heating valves (for RTU/AHU analytics)
        # ---------------------------
        {
            "role": "cooling_valve",
            "priority": 10,
            "name_regex": [
                r"cooling\s*valve",
                r"chilled\s*water\s*valve",
                r"cw\s*valve",
            ],
            "tags_all": [],
            "tags_any": [],
        },
        {
            "role": "heating_valve",
            "priority": 10,
            "name_regex": [
                r"heating\s*valve",
                r"hw\s*valve",
                r"boiler\s*valve",
            ],
            "tags_all": [],
            "tags_any": [],
        },

        # ---------------------------
        # Compressors (RTU)
        # ---------------------------
        {
            "role": "compressor_cmd",
            "priority": 10,
            "name_regex": [
                r"compressor\s*\d*\s*(cmd|command|output)",
                r"stage\s*\d*\s*(cmd|command|output)",
                r"cool\s*\d*\s*(cmd|command|output)",
            ],
            "tags_all": [],
            "tags_any": [],
        },
        {
            "role": "compressor_status",
            "priority": 10,
            "name_regex": [
                r"compressor\s*\d*\s*status",
                r"stage\s*\d*\s*status",
                r"compressor\s*\d*\s*proof",
                r"stage\s*\d*\s*proof",
            ],
            "tags_all": [],
            "tags_any": [],
        },
    ]
}


# ---------------------------------------------------------------------------
# Loading rules from JSON file (config/role_rules.json)
# ---------------------------------------------------------------------------


def _load_rules_from_file(path: Path) -> List[RoleRule]:
    """
    Load role rules from a JSON file with structure:

        {
          "rules": [
            {
              "role": "space_temp",
              "priority": 5,
              "name_regex": [...],
              "tags_all": [...],
              "tags_any": [...]
            },
            ...
          ]
        }
    """
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    specs: Sequence[Dict[str, Any]] = raw.get("rules", [])
    rules: List[RoleRule] = []
    for spec in specs:
        rules.append(
            RoleRule(
                role=str(spec["role"]),
                name_regex=list(spec.get("name_regex", []) or []),
                tags_all=list(spec.get("tags_all", []) or []),
                tags_any=list(spec.get("tags_any", []) or []),
                priority=int(spec.get("priority", 100)),
            )
        )
    return rules


def _default_rules() -> List[RoleRule]:
    rules: List[RoleRule] = []
    for spec in _DEFAULT_RULES_SPEC.get("rules", []):
        rules.append(
            RoleRule(
                role=str(spec["role"]),
                name_regex=list(spec.get("name_regex", []) or []),
                tags_all=list(spec.get("tags_all", []) or []),
                tags_any=list(spec.get("tags_any", []) or []),
                priority=int(spec.get("priority", 100)),
            )
        )
    return rules


@lru_cache(maxsize=1)
def get_rules() -> List[RoleRule]:
    """
    Load role rules from config/role_rules.json if present; otherwise
    fall back to built-in defaults.
    """
    here = Path(__file__).resolve()
    project_root = here.parents[2]
    cfg_path = project_root / "config" / "role_rules.json"

    try:
        rules = _load_rules_from_file(cfg_path)
        print(f"[role_rules] Loaded {len(rules)} rules from {cfg_path}")
        return rules
    except FileNotFoundError:
        print(f"[role_rules] No role_rules.json at {cfg_path}, using built-in defaults")
    except Exception as e:  # noqa: BLE001
        print(f"[role_rules] Failed to load role_rules.json: {e}, using defaults")

    return _default_rules()


# ---------------------------------------------------------------------------
# Public API: infer role from label + tags
# ---------------------------------------------------------------------------


def infer_role(label: str, tags: Optional[List[Any]] = None) -> Optional[str]:
    """
    Infer the analytic role of a point from its name and tags using the
    configured rule set (JSON or built-in defaults).

    Args:
        label: point display name or history id
        tags:  list of tag suffixes, e.g. ["air", "flow", "zone", "sp"]

    Returns:
        role string such as "space_temp", "space_temp_sp", "flow", "flow_sp",
        "damper", "reheat", "fan_cmd", "fan_status", etc., or None if
        no rule matches.
    """
    rules = get_rules()

    tag_set: Optional[Set[str]] = None
    if tags:
        tag_set = {str(t).lower() for t in tags}

    best: Optional[Tuple[int, RoleRule]] = None
    for rule in rules:
        if rule.matches(label, tag_set):
            if best is None or rule.priority < best[0]:
                best = (rule.priority, rule)

    if best is None:
        return None

    return best[1].role
