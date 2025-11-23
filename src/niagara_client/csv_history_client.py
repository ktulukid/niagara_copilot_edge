from __future__ import annotations

import csv
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict

from pydantic import BaseModel


class HistorySample(BaseModel):
    ts: datetime
    value: float
    status: Optional[str] = None


class CsvHistoryClient:
    """
    Loads one or more Niagara history CSV files from a directory.

    Automatically detects timestamp format:
      e.g. "22-Nov-25 12:00:00 AM MST"

    Automatically strips:
      - status braces: "{ok}" -> "ok"
      - trend flags: "{ }" -> ""
      - units: "72.0 °F" -> 72.0

    Query via:
        get_history(point_name="AmsShop/Vav1-11 SpaceTemperature", hours=24)
    """

    TIMESTAMP_REGEX = re.compile(
        r"(?P<day>\d{2})-(?P<mon>[A-Za-z]{3})-(?P<yr>\d{2})\s+"
        r"(?P<hour>\d{1,2}):(?P<min>\d{2}):(?P<sec>\d{2})\s+"
        r"(?P<ampm>AM|PM)\s+(?P<tz>[A-Z]{2,4})"
    )

    UNIT_REGEX = re.compile(r"([0-9\.\-]+)")

    def __init__(self, directory: str):
        self.directory = Path(directory)
        if not self.directory.exists():
            raise ValueError(f"CSV directory not found: {directory}")

        self._cache: Dict[str, List[HistorySample]] = {}

    # ----------------------------------------------------------------------

    def _parse_timestamp(self, raw: str) -> datetime:
        """
        Convert Niagara timestamp string -> aware datetime.
        Example: "22-Nov-25 12:05:00 AM MST"
        """
        m = self.TIMESTAMP_REGEX.match(raw.strip())
        if not m:
            raise ValueError(f"Unrecognized timestamp format: {raw}")

        parts = m.groupdict()
        day = int(parts["day"])
        year = 2000 + int(parts["yr"])  # "25" -> 2025
        hour = int(parts["hour"])
        minute = int(parts["min"])
        second = int(parts["sec"])
        ampm = parts["ampm"]
        tz = parts["tz"]

        # Convert 12h -> 24h
        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0

        return datetime.strptime(
            f"{day}-{parts['mon']}-{year} {hour:02d}:{minute:02d}:{second:02d} {tz}",
            "%d-%b-%Y %H:%M:%S %Z"
        )

    # ----------------------------------------------------------------------

    def _strip_status(self, raw: str) -> str:
        return raw.strip("{} ").strip() or None

    def _strip_units(self, raw: str) -> float:
        """
        Extract numeric value from strings like "72.0 °F"
        """
        m = self.UNIT_REGEX.search(raw)
        if not m:
            raise ValueError(f"Could not parse numeric value from: {raw}")
        return float(m.group(1))

    # ----------------------------------------------------------------------

    def _load_file(self, csv_path: Path) -> List[HistorySample]:
        samples = []

        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)

            # Skip header lines until we reach the columns row
            for row in reader:
                if row and row[0].strip().lower() == "timestamp":
                    headers = row
                    break

            # Build column index map
            idx_ts = headers.index("Timestamp")
            idx_status = headers.index("Status")
            # Value column might be "Value (°F)" or "Value" etc.
            idx_value = [i for i, h in enumerate(headers) if "Value" in h][0]

            for row in reader:
                if not row or not row[idx_ts].strip():
                    continue

                ts = self._parse_timestamp(row[idx_ts])
                status = self._strip_status(row[idx_status])
                value = self._strip_units(row[idx_value])

                samples.append(HistorySample(ts=ts, value=value, status=status))

        return samples

    # ----------------------------------------------------------------------

    def _find_history_file(self, point_name: str) -> Optional[Path]:
        """
        Tries to find a CSV whose filename contains any part of the point name.
        e.g. point_name="Vav1-11 SpaceTemperature"
             matches: "vav_1_11_space_temp.csv"
        """
        name_key = point_name.lower().replace(" ", "").replace("-", "").replace("_", "")

        for file in self.directory.glob("*.csv"):
            processed = file.stem.lower().replace(" ", "").replace("-", "").replace("_", "")
            if name_key in processed or processed in name_key:
                return file

        return None

    # ----------------------------------------------------------------------

    def get_history(
        self,
        point_name: str,
        hours: Optional[int] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[HistorySample]:
        """
        Load and optionally filter history.

        Examples:
            get_history("Vav1-11 SpaceTemperature", hours=24)
            get_history("AHU-1 SAT", start=..., end=...)
        """

        if point_name not in self._cache:
            path = self._find_history_file(point_name)
            if not path:
                raise FileNotFoundError(
                    f"No CSV found in {self.directory} for point '{point_name}'"
                )
            self._cache[point_name] = self._load_file(path)

        samples = self._cache[point_name]

        # Filter by time
        if hours is not None:
            cutoff = datetime.now() - timedelta(hours=hours)
            return [s for s in samples if s.ts >= cutoff]

        if start is not None or end is not None:
            start = start or datetime.min
            end = end or datetime.max
            return [s for s in samples if start <= s.ts <= end]

        return samples
