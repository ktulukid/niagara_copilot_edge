from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    # pyhaystack Niagara client
    from pyhaystack.client.niagara import NiagaraHaystackSession
except ImportError as e:  # noqa: BLE001
    # Make the error obvious at import time if pyhaystack is missing
    raise ImportError(
        "pyhaystack is required for HaystackHistoryClient. "
        "Install with: pip install pyhaystack"
    ) from e


@dataclass
class HaystackConfig:
    """
    Configuration for connecting to a Haystack (Niagara+nHaystack) server.

    uri:       Base URI of the station, e.g. 'http://172.20.40.22'
    username:  Niagara / nHaystack username
    password:  Password for the user
    proj:      Optional project/station hint (not used by NiagaraHaystackSession)
    """

    uri: str
    username: str
    password: str
    proj: str = "default"


class HaystackHistoryClient:
    """
    Minimal wrapper around pyhaystack for:

      - Discovering entities via tag filters (read_by_filter)
      - Reading history for a point (his_read)

    Designed to be:

      - Simple to plug into Niagara Copilot Edge
      - Compatible with the HistoryClient protocol in niagara_client.factory
    """

    def __init__(self, cfg: HaystackConfig) -> None:
        self._cfg = cfg

        # NiagaraHaystackSession handles auth, cookies, async ops, etc.
        # We use it in a synchronous style (op.wait(); op.result).
        self._session = NiagaraHaystackSession(
            uri=cfg.uri,
            username=cfg.username,
            password=cfg.password,
            pint=False,
        )

    # -------------------------------------------------------------------------
    # Core API (matches HistoryClient protocol)
    # -------------------------------------------------------------------------

    def read_by_filter(self, filter_expr: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Run a Haystack `read` operation with a filter expression.

        Returns:
            List of dictionaries; each dict is a row of tags for one entity.

        Important: we special-case 'id' so you always get a usable string.
        """
        op = self._session.read(filter_expr=filter_expr, limit=limit)
        op.wait()
        grid = op.result

        rows: List[Dict[str, Any]] = []
        for row in grid:
            row_dict: Dict[str, Any] = {}
            for k, v in row.items():
                # 'id' is usually an hszinc Ref; we want a stable string like '@<id>'
                if k == "id":
                    if hasattr(v, "value") and v.value is not None:
                        row_dict["id"] = f"@{v.value}"
                    else:
                        row_dict["id"] = str(v)
                    continue

                # For other wrapped types, prefer .value (e.g. Quantity, Ref, DateTime)
                if hasattr(v, "value"):
                    val = v.value
                else:
                    val = v

                row_dict[k] = val
            rows.append(row_dict)

        return rows

    def his_read(self, entity_id: str, range_str: str) -> List[Tuple[datetime, float]]:
        """
        Read history for a historized point by its Haystack id and a range string.

        Args:
            entity_id: Haystack point id (string), e.g. '@S.AmsShop.Vav1_01.SpaceTemperature'
                       or 'S.AmsShop.Vav1_01.SpaceTemperature'
            range_str: Haystack range string, e.g.:
                       - 'today'
                       - 'yesterday'
                       - '2025-11-27,2025-11-28'
                       - '2025-11-27T00:00,2025-11-27T23:59'

        Returns:
            List of (timestamp, value) tuples.
        """
        # pyhaystack docs: point can be the ID string of the historical point entity
        if isinstance(entity_id, str) and entity_id.startswith("@"):
            point_id = entity_id[1:]  # strip leading '@'
        else:
            point_id = entity_id

        op = self._session.his_read(point=point_id, rng=range_str)
        op.wait()
        grid = op.result

        samples: List[Tuple[datetime, float]] = []
        for row in grid:
            ts = row["ts"]
            val = row["val"]

            if hasattr(ts, "value"):
                ts = ts.value
            if hasattr(val, "value"):
                val = val.value

            samples.append((ts, val))

        return samples

    # -------------------------------------------------------------------------
    # Convenience helpers
    # -------------------------------------------------------------------------

    def find_zone_temp_points(
        self,
        site_ref: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Convenience helper: find all zone temp points.

        If site_ref is provided, it should be the bare id (without leading '@'),
        and we filter on `siteRef==@<site_ref>`.
        """
        if site_ref:
            filter_expr = f"point and zone and temp and siteRef==@{site_ref}"
        else:
            filter_expr = "point and zone and temp"

        return self.read_by_filter(filter_expr, limit=limit)
