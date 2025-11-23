from datetime import datetime, timedelta

from fastapi import FastAPI, Query
from pydantic import BaseModel

from ..config import load_config
from ..niagara_client.csv_stub import CsvHistoryClient
from ..analytics.comfort import compute_zone_comfort

app = FastAPI(title="Niagara Copilot Edge (Demo)")

_config = load_config()
_history_client = CsvHistoryClient(_config)


class ComfortResponse(BaseModel):
    site: str
    equip: str
    start: datetime
    end: datetime
    samples: int
    within_band_pct: float | None
    mean_error_degF: float | None


@app.get("/health")
def health():
    return {
        "status": "ok",
        "site": _config.site_name,
    }


@app.get("/comfort", response_model=ComfortResponse)
def comfort(
    equip: str = Query(..., description="Equipment/zone identifier, e.g. Zone-101"),
    hours: int = Query(24, ge=1, le=168, description="Lookback window in hours"),
):
    end = datetime.now()
    start = end - timedelta(hours=hours)

    df = _history_client.get_zone_history(equip=equip, start=start, end=end)
    metrics = compute_zone_comfort(df, _config.comfort)

    return ComfortResponse(
        site=_config.site_name,
        equip=equip,
        start=start,
        end=end,
        samples=metrics["samples"],
        within_band_pct=metrics["within_band_pct"],
        mean_error_degF=metrics["mean_error_degF"],
    )
