from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List
import os
import re
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from ..config import AppConfig, NiagaraCsvExportConfig, ComfortConfig

# Match any link that references a CSV file
CSV_LINK_RE = re.compile(r'href="([^"]*\.csv[^"]*)"', re.IGNORECASE)


@dataclass
class NiagaraCsvExportClient:
    cfg: AppConfig
    _comfort: ComfortConfig
    _niagara: NiagaraCsvExportConfig

    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        if cfg.data_source.niagara_csv_export is None:
            raise ValueError("niagara_csv_export config not provided")
        self._niagara = cfg.data_source.niagara_csv_export
        self._comfort = cfg.comfort

        # TLS verification (False when using self-signed JACE certs)
        self._verify_tls = not self._niagara.insecure_tls

        pwd = os.getenv(self._niagara.password_env)
        if not pwd:
            raise RuntimeError(
                f"Environment variable {self._niagara.password_env} is not set "
                "for Niagara CSV export authentication."
            )

        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(self._niagara.username, pwd)

    # ---------------- internal helpers ----------------

    def _station_name_from_ord(self) -> str:
        """
        Extract station name from ord_path.
        Expect ord_path like: 'file:^historyExports/AmsShop'
        """
        ord_path = self._niagara.ord_path
        m = re.match(r"file:\^historyExports/([^/]+)$", ord_path)
        if not m:
            raise RuntimeError(
                f"ord_path '{ord_path}' is not in expected form 'file:^historyExports/<stationName>'"
            )
        return m.group(1)

    def _directory_url(self) -> str:
        """
        Use the /file servlet instead of /ord nav shell.

        For station 'AmsShop', build:
        https://<host>/file/AmsShop/historyExports/AmsShop/
        """
        station = self._station_name_from_ord()
        return f"https://{self._niagara.host}/file/{station}/historyExports/{station}/"

    def _fetch_html(self, url: str) -> str:
        resp = self._session.get(url, verify=self._verify_tls)
        resp.raise_for_status()
        return resp.text

    def _list_csv_urls(self) -> List[str]:
        """Return all CSV hrefs found in the Niagara historyExports directory HTML."""
        url = self._directory_url()
        html = self._fetch_html(url)

        # ------------ DEBUG START ------------
        print("\n===== DEBUG _list_csv_urls =====")
        print("Directory URL:", url)
        print("HTML length:", len(html))
        print("HTML preview:\n", html[:1500])
        print("===== END HTML PREVIEW =====\n")
        # ------------ DEBUG END --------------

        hrefs = CSV_LINK_RE.findall(html)
        print("DEBUG: CSV_LINK_RE matches:", hrefs)

        if not hrefs:
            raise RuntimeError(
                f"No CSV links found at {url}. "
                "Check that this URL shows a directory listing with CSV files when opened in a browser."
            )

        # Make all hrefs absolute URLs
        return [urljoin(url, href) for href in hrefs]

    def _download_csv(self, url: str) -> pd.DataFrame:
        resp = self._session.get(url, verify=self._verify_tls)
        resp.raise_for_status()
        return pd.read_csv(pd.io.common.BytesIO(resp.content))

    # ---------------- public API ----------------

    def get_zone_history(
        self,
        equip: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """
        Load all CSVs from the configured Niagara historyExports/<station> folder,
        merge them, then filter by equip + time range according to comfort config.
        """
        csv_urls = self._list_csv_urls()

        dfs: list[pd.DataFrame] = []
        for url in csv_urls:
            df = self._download_csv(url)
            dfs.append(df)

        merged = pd.concat(dfs, ignore_index=True)

        c = self._comfort
        t_col = c.timestamp_column
        e_col = c.equip_column

        # Parse timestamps if not already datetime
        if not pd.api.types.is_datetime64_any_dtype(merged[t_col]):
            merged[t_col] = pd.to_datetime(merged[t_col])

        mask = (
            (merged[e_col] == equip)
            & (merged[t_col] >= start)
            & (merged[t_col] <= end)
        )
        return merged.loc[mask].copy()
