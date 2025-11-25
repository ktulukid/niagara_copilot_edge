# src/niagara_client/niagara_servlet_client.py
from dataclasses import dataclass
from datetime import datetime
from typing import List
import os

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from ..config import AppConfig, NiagaraCsvExportConfig, ComfortConfig  # reuse existing types


@dataclass
class NiagaraHistoryServletClient:
    cfg: AppConfig
    _comfort: ComfortConfig
    _niagara: NiagaraCsvExportConfig  # reuse: host/username/password_env/insecure_tls

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        if cfg.data_source.niagara_csv_export is None:
            raise ValueError("niagara_csv_export config not provided")
        self._niagara = cfg.data_source.niagara_csv_export
        self._comfort = cfg.comfort

        pwd = os.getenv(self._niagara.password_env)
        if not pwd:
            raise RuntimeError(
                f"Environment variable {self._niagara.password_env} is not set "
                "for Niagara servlet authentication."
            )

        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(self._niagara.username, pwd)
        self._verify_tls = not self._niagara.insecure_tls

    def _history_url(self) -> str:
        # Your servlet path
        return f"https://{self._niagara.host}/niagaraCopilot"

    def _fetch_raw_history(self) -> dict:
        resp = self._session.get(self._history_url(), verify=self._verify_tls)
        resp.raise_for_status()
        return resp.json()

    def get_zone_history(self, equip: str, start: datetime, end: datetime) -> pd.DataFrame:
        """
        For now: pull the one history your servlet returns
        and let the comfort logic work on that series.
        Later we’ll add parameters for history/equip/range.
        """
        payload = self._fetch_raw_history()
        rows = payload.get("historyData", [])

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        c = self._comfort
        t_col = c.timestamp_column

        # your JSON field names: timestamp, value, status, trendFlags
        df[t_col] = pd.to_datetime(df["timestamp"])
        df.rename(columns={"value": c.value_column}, inplace=True)

        # optional: apply time window here
        mask = (df[t_col] >= start) & (df[t_col] <= end)
        return df.loc[mask].reset_index(drop=True)
