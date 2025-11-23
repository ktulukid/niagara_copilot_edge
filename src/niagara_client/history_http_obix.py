from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

import requests
import pandas as pd
from xml.etree import ElementTree as ET


@dataclass
class HistorySample:
    timestamp: datetime
    value: float


class NiagaraObixHistoryClient:
    """
    Simple client to read Niagara histories via the oBIX historyQuery endpoint.

    base_url example: "http://172.20.40.22/obix"
    history_query_path example:
        "/histories/AmsShop/Vav1-11-SpaceTemperature/-/historyQuery"
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 10,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")  # .../obix
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        sess = requests.Session()
        sess.auth = (username, password)  # HTTP Basic
        self.session = sess

    def fetch_history(
        self,
        history_query_path: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """
        Call oBIX historyQuery and return a DataFrame with columns:
        ['timestamp', 'value'].
        """

        # Normalise path and build URL
        path = history_query_path
        if not path.startswith("/"):
            path = "/" + path

        url = self.base_url + path

        params = {
            "start": start.isoformat(),  # e.g. 2025-11-22T00:00:00-07:00
            "end": end.isoformat(),
        }

        resp = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()

        # Parse oBIX XML
        root = ET.fromstring(resp.content)

        rows: List[Dict[str, Any]] = []

        # Look for <list name="data"> of obix:HistoryRecord objects
        for obj in root.findall(".//{*}list[@name='data']/{*}obj"):
            ts_el = obj.find("{*}abstime[@name='timestamp']")
            val_el = obj.find("{*}real[@name='value']")

            if ts_el is None or val_el is None:
                continue

            ts_str = ts_el.get("val")
            val_str = val_el.get("val")

            if ts_str is None or val_str is None:
                continue

            # Parse timestamp; Niag often includes timezone offset
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                # Fallback – naive ISO without offset
                ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")

            try:
                value = float(val_str)
            except ValueError:
                continue

            rows.append({"timestamp": ts, "value": value})

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = df.sort_values("timestamp").reset_index(drop=True)
        return df
