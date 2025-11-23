from datetime import datetime
from pathlib import Path

import pandas as pd

from ..config import AppConfig


class CsvHistoryClient:
    """
    Simple CSV-based history client.
    Loads a single CSV into memory and filters by equip + time range.
    """

    def __init__(self, app_config: AppConfig) -> None:
        ds = app_config.data_source
        if ds.type != "csv":
            raise ValueError("CsvHistoryClient requires data_source.type == 'csv'")
        if not ds.path:
            raise ValueError("data_source.path is required for csv type")

        self._cfg = app_config
        self._csv_path = Path(ds.path)

        if not self._csv_path.exists():
            raise FileNotFoundError(self._csv_path)

        # Load once
        self._df = pd.read_csv(self._csv_path)

        # Normalize timestamp column
        ts_col = self._cfg.comfort.timestamp_column
        self._df[ts_col] = pd.to_datetime(self._df[ts_col])

    def get_zone_history(
        self,
        equip: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        c = self._cfg.comfort
        df = self._df

        mask = (
            (df[c.equip_column] == equip)
            & (df[c.timestamp_column] >= start)
            & (df[c.timestamp_column] <= end)
        )
        return df.loc[mask].copy()
