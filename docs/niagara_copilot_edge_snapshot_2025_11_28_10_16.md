# Niagara Copilot Edge Snapshot — 2025-11-27

## 1. Project Summary
- Niagara Copilot Edge ingests Niagara history data via an MQTT `niagara/histories` feed parsed by `niagara_client.mqtt_history_ingest.make_history_mqtt_client`, optionally via CSV exports, HTTP oBIX, or custom servlet clients when configured.
- Incoming payloads become `HistorySample` dataclasses (station, history id, timestamp, status, value) that feed both an in-memory `history_store` (capped per series) and the SQLite-backed `history_samples` table for persistence.
- Canonicalizers (`niagara_decode_name`, `niagara_canonical_name`) plus regex-driven role detection group histories into zone/equipment “pairs” so analytics know which temperature, flow, damper, reheat, and fan signals belong together.
- Analytics modules compute comfort deviation, flow tracking, damper/reheat sanity, and a weighted zone health score based on configurable comfort windows, merge tolerances, and tracking thresholds.
- A FastAPI server (`src.api.server`) exposes health, debug, and summary endpoints that pull slices of `sqlite_store` data, run the analytics pipeline, and return JSON-friendly Pydantic models.
- Configuration (`src.config`) drives every layer—data source selection, MQTT connectivity, comfort thresholds, and SQLite retention—so deployments remain site-specific yet predictable.

## 2. Directory and Module Map
```
src/
├── analytics/
│   ├── comfort.py
│   ├── flow.py
│   ├── zone_pairs.py
│   └── zone_health.py
├── api/
│   └── server.py
├── niagara_client/
│   ├── analytics_api.py
│   ├── factory.py
│   ├── history_http_obix.py
│   ├── mqtt_history_client.py
│   ├── mqtt_history_ingest.py
│   ├── niagara_csv_export_client.py
│   └── niagara_servlet_client.py
├── store/
│   ├── history_store.py
│   └── sqlite_store.py
├── config.py
└── main.py
```
- `src/analytics/comfort.py` exposes `compute_zone_comfort(df, comfort_cfg)` to align a DataFrame of paired temperature/setpoint values with the configured occupied window and comfort band, returning sample counts, percent within band, and mean error.
- `src/analytics/flow.py` defines `FlowTrackingConfig` (timestamp/value column names, percent tolerance, optional abs CFM tolerance, merge tolerance) and `compute_flow_tracking(df_flow, df_flow_sp, cfg)` to merge flow vs setpoint, calculate error %, and report coverage metrics.
- `src/analytics/zone_pairs.py` detects equipment roots via `ZONE_RE`, matches role patterns from `ROLE_PATTERNS`, and builds `ZonePair` dataclasses through `build_zone_pair_index`, with helpers `find_zone_pair` and `zone_pairs_as_dicts` for API filtering.
- `src/analytics/zone_health.py` orchestrates `_compute_comfort`, `_compute_flow_and_damper`, `_compute_reheat_waste`, and `_compute_overall_score` to populate `ZoneHealthMetrics`, exposed through `compute_zone_health` and `zone_health_to_dict`.
- `src/api/server.py` wires FastAPI routes (`/health`, debug endpoints, summary endpoints) to the config, MQTT callbacks, history stores, and analytics functions, defining response models such as `ZoneHealthMetricsModel`, `FlowTrackingResponse`, and `ComfortZonePairResponse`.
- `src/niagara_client/analytics_api.py` and `niagara_servlet_client.py` offer Nike/HTTP clients for the Niagara analytics servlet; neither is wired into the default factory but they expose HTTP helpers (`AnalyticsApiClient`, `NiagaraHistoryServletClient`, `NiagaraCsvExportClient`) that fetch JSON/CSV for comfort computations.
- `src/niagara_client/history_http_obix.py` provides `NiagaraObixHistoryClient.fetch_history` for oBIX historyQuery XML, while `mqtt_history_client.py` keeps MQTT JSON history records in a rolling in-memory cache keyed by the comfort config’s timestamp + equip columns.
- `src/niagara_client/mqtt_history_ingest.py` defines the `HistorySample` dataclass, MQTT parsing/validation helpers (`niagara_decode_name`, `_validate_history_frame`, `_parse_timestamp`), and `make_history_mqtt_client` which emits batches to a callback.
- `src/niagara_client/niagara_csv_export_client.py` enumerates CSV links from `historyExports/<station>` via HTTP basic auth and `requests`, filters by equipment/time using pandas, and returns the merged DataFrame for a comfort zone.
- `src/store/history_store.py` maintains a per-series in-memory dict limited to `_MAX_PER_SERIES` samples, exposes `add_batch`, `clear`, and `get_recent`.
- `src/store/sqlite_store.py` is the durable store: it init’s the SQLite DB (`history_samples` table plus `idx_history_samples_ts`/`idx_history_samples_series`), supports `add_batch` (INSERT OR REPLACE, retention pruning), and provides `list_series`/`query_series` APIs used by analytics and endpoints.
- `src/config.py` defines the Pydantic models `AppConfig`, `DataSourceConfig`, `ComfortConfig`, `MqttConfig`, `MqttJsonStreamConfig`, and `NiagaraCsvExportConfig`, plus `load_config` that reads YAML and optionally prompts for MQTT overrides.
- `src/main.py` simply launches Uvicorn pointing at `src.api.server:app` for local development.

## 3. Configuration Models
- `AppConfig`
  - `site_name: str` – identifies the deployment/site (used for logging/documentation).
  - `data_source: DataSourceConfig` – selects the ingestion path (currently MQTT stream or CSV export).
  - `comfort: ComfortConfig` – holds columns and thresholds for comfort scoring.
  - `mqtt: MqttConfig = MqttConfig()` – MQTT broker host/port/topic for the Niagara history feed consumed by `make_history_mqtt_client`.
  - `db_path: str = "data/history.sqlite"` – filesystem path to the SQLite history store.
  - `db_retention_hours: int = 720` – rolling retention window applied by `sqlite_store._apply_retention`.
- `DataSourceConfig`
  - `type: Literal["niagara_csv_export", "mqtt_json_stream"]` – determines which nested config is active.
  - `niagara_csv_export: Optional[NiagaraCsvExportConfig]` – HTTP Basic CSV export settings (required when using CSV ingestion).
  - `mqtt_json_stream: Optional[MqttJsonStreamConfig]` – JSON MQTT stream client settings.
- `ComfortConfig`
  - `occupied_start`, `occupied_end: str` (e.g., `"07:00"`/`"18:00"`) – define the daytime window for comfort scoring.
  - `setpoint_column`, `temp_column`, `timestamp_column`, `equip_column: str` – column names that the CSV/JSON sources use for setpoints, temperatures, timestamps, and equipment identifiers.
  - `comfort_band_degF: float` – allowed deviation from setpoint during occupied hours for `compute_zone_comfort`.
- `MqttConfig`
  - `host: str = "localhost"` – where `make_history_mqtt_client` connects.
  - `port: int = 1883` – MQTT broker port (overridable interactively or via env vars `MQTT_HOST`/`MQTT_PORT`).
  - `history_topic: str = "niagara/histories"` – MQTT topic subscribed for Niagara history frames.
- `MqttJsonStreamConfig`
  - `host: str`, `port: int = 8883`, `topic: str` – broker info for `MqttHistoryClient`.
  - `username: Optional[str]`, `password_env: Optional[str]` – credentials retrieved from environment variables.
  - `tls: bool = True`, `client_id: Optional[str]`, `keepalive: int = 60` – MQTT connection parameters.
  - `retention_hours: int = 24` – how long the rolling cache in `MqttHistoryClient` keeps records.
- `NiagaraCsvExportConfig`
  - `host: str`, `ord_path: str` (e.g., `"file:%5EhistoryExports/AmsShop"`) – points to the station export directory / servlet.
  - `username: str`, `password_env: str = "NIAGARA_PASSWORD"` – HTTP Basic auth, password fetched from env.
  - `insecure_tls: bool = True` – when Niagaras use self-signed certs; passed to `requests`.
- `load_config(path)` – parses `config/config.yaml` into these models, then prompts interactively for MQTT host/port overrides if stdin is available, falling back silently otherwise; this config is held globally ( `_config`) by `api.server`.

## 4. Data Flow: Ingestion → Storage → Analytics → API
- **Ingestion:** `make_history_mqtt_client` subscribes to the configured MQTT topic, validates JSON via `_validate_history_frame`, decodes timestamps like `2025-11-24 00:00:01.349-0700`, and emits `HistorySample(station_name, history_id, timestamp, status, value)` batches to `_on_history_batch`. `niagara_decode_name` and `niagara_canonical_name` turn Niagara labels (`Zone$2d1$20Space$20Temp`) into human-friendly strings and machine-safe keys. Alternative clients (`NiagaraCsvExportClient`, `NiagaraHistoryServletClient`, `NiagaraObixHistoryClient`, and `MqttHistoryClient`) fetch data over HTTP/CSV and return pandas DataFrames aligned to the configured comfort columns, enabling the same analytics pipeline if hooked up.
- **Storage:** `_on_history_batch` writes to `store.history_store` for fast debugging (per-series cap `_MAX_PER_SERIES = 1000`) and to `store.sqlite_store`. `sqlite_store.add_batch` calculates canonical station/history keys, converts timestamps to UTC (ISO strings), and performs `INSERT OR REPLACE` (so repeated windows overwrite) before committing. `_apply_retention` deletes rows older than `db_retention_hours`.
- **Querying:** `sqlite_store.list_series` returns distinct `(station_key, history_key, station_name, history_id)` for pairing. `sqlite_store.query_series` filters by station/history/time range, uses canonical keys, orders by `ts`, and returns JSON-ready rows that `api.server._rows_to_dataframe` converts into pandas DataFrames for analytics.
- **Analytics:** `zone_pairs.zone_pairs_as_dicts` and `find_zone_pair` scan `list_series` results, infer zone roots with `ZONE_RE`, assign roles via `ROLE_PATTERNS`, and populate `ZonePair` metadata. `compute_zone_comfort` merges temperature and setpoint data within the occupied window. `compute_flow_tracking` merges flow vs flow setpoint (respecting `FlowTrackingConfig.merge_tolerance_seconds`) and reports samples, within-band pct, and mean errors. `compute_zone_health` coordinates `_compute_comfort`, `_compute_flow_and_damper`, `_compute_reheat_waste`, and `_compute_overall_score` to yield comfort/flow/damper/reheat metrics plus an `overall_score`; `zone_health_to_dict` flattens the dataclass for JSON responses.
- **API Exposure:** FastAPI routes call `sqlite_store` queries and analytics helpers (`compute_zone_comfort`, `compute_flow_tracking`, `compute_zone_health`, zone pair helpers) to power `/debug/*` and `/summary/*` endpoints. The MQTT client runs in the module scope, so incoming batches immediately update the stores that serve these routes.

## 5. Database Schema
- `history_samples` table (created in `store.sqlite_store._init_schema`):
  - `station_key TEXT NOT NULL` – canonical station name (`niagara_canonical_name`), part of the PK.
  - `history_key TEXT NOT NULL` – canonical history label, part of the PK.
  - `station_name TEXT NOT NULL` – original station label for human display.
  - `history_id TEXT NOT NULL` – decoded history label (readable path).
  - `ts TEXT NOT NULL` – ISO 8601 timestamp in UTC (`HistorySample.timestamp.astimezone(timezone.utc).isoformat()`).
  - `status TEXT` – optional status tags from Niagara.
  - `value REAL NOT NULL` – numeric measurement (floats).
  - Primary key: `(station_key, history_key, ts)` enforces one sample per timestamp per series.
- Indexes:
  - `idx_history_samples_ts` on `ts` accelerates time range pruning and retention DELETEs.
  - `idx_history_samples_series` on `(station_key, history_key, ts)` speeds `query_series`.
- `add_batch` writes rows via `conn.executemany` and commits; `_apply_retention` issues `DELETE FROM history_samples WHERE ts < ?`.
- `list_series`/`query_series` read this table, and all analytics (`zone_pairs`, `zone_health`, `compute_flow_tracking`) consume `query_series` output.

## 6. API Endpoints
- `GET /health` – returns `HealthResponse(status="ok")`; used for readiness checks.
- `GET /debug/recent_memory` – Query params `station`, `history_id`, `limit`. Calls `store.history_store.get_recent` and returns `HistorySampleJson` objects (`stationName`, `historyId`, `timestamp`, `status`, `value`), all from the in-memory cache.
- `GET /debug/comfort_zone_pair` – Params `station`, `temp_history_id`, `sp_history_id`, `hours`, `merge_tolerance_seconds`. Queries both histories via `sqlite_store.query_series`, converts to DataFrames, aligns them with pandas `merge_asof` (tolerance `merge_tolerance_seconds`), and feeds the result to `compute_zone_comfort`. Returns `ComfortZonePairResponse` (`history_temp_id`, `history_sp_id`, `metrics`: `samples`, `within_band_pct`, `mean_error_degF`).
- `GET /debug/zone_pairs` – Optional `station`, `zone` filters. Uses `zone_pairs_as_dicts`, canonicalizes filters via `niagara_canonical_name`, and returns `ZonePairResponse` entries that list `space_temp`, `flow`, `damper`, `reheat`, `fan_cmd`, `fan_status`, etc.
- `GET /debug/flow_tracking` – Params `station`, `zone`, optional `start`, `end`. Finds a zone pair via `find_zone_pair`, queries flow/flow setpoint histories, merges with `_rows_to_dataframe`, runs `compute_flow_tracking` (using default `FlowTrackingConfig`), and returns `FlowTrackingResponse` (`metrics` dictionary, `station`, `zone`, history IDs, `start`, `end`).
- `GET /summary/zone_health` – Params `station`, `zone`, `hours`. Pulls metadata via `zone_pairs_as_dicts`, computes `ZoneHealthMetrics` with `compute_zone_health`, converts via `zone_health_to_dict`, and returns `ZoneHealthMetricsModel` (comfort/flow/damper/reheat metrics plus `overall_score`) for the requested zone.
- `GET /summary/building_health` – Params `station`, `hours`. Iterates every zone pair for the station, runs `compute_zone_health`, sorts by `overall_score` (worst first), and returns a list of `ZoneHealthMetricsModel` to show the station-level health ranking.

## 7. Analytics Functions
1. `compute_zone_comfort(df: pd.DataFrame, comfort_cfg: ComfortConfig) -> dict`  
   - `df`: merged temperature + setpoint rows with columns named by `comfort_cfg.timestamp_column`, `comfort_cfg.temp_column`, and `comfort_cfg.setpoint_column`.  
   - `comfort_cfg`: defines occupied start/end and `comfort_band_degF`.  
   - Returns `{"samples": int, "within_band_pct": float | None, "mean_error_degF": float | None}` calculated only over data points whose timestamps fall within the configured occupied window.
2. `compute_flow_tracking(df_flow: pd.DataFrame, df_flow_sp: Optional[pd.DataFrame], cfg: FlowTrackingConfig | None = None) -> dict`  
   - Inputs expect columns (`timestamp_column`, `value_column`) defined by `cfg` (defaults to `"timestamp"`/`"value"`) and join with `merge_asof` within `cfg.merge_tolerance_seconds` (default 30s).  
   - Computes `error_cfm`, percent error, and whether each sample is within tolerance (max of `%` tolerance and optional absolute CFM tolerance).  
   - Returns `{"samples": int, "within_band_pct": float | None, "mean_error_cfm": float | None, "mean_error_pct": float | None}` for API metrics.
3. `compute_zone_health(station: str, zone_root: str, zone_info: Dict[str, Any], comfort_cfg: ComfortConfig, start: Optional[datetime] = None, end: Optional[datetime] = None) -> ZoneHealthMetrics`  
   - Orchestrates `_compute_comfort`, `_compute_flow_and_damper`, `_compute_reheat_waste`, and `_compute_overall_score`.  
   - Pulls histories via `_query_series_df`, merges temperature/sp/setpoint/flow/damper/reheat streams with 30s tolerances, and reports comfort/flow metrics plus damper/reheat failure percentages and an `overall_score` (higher is better, weighted across comfort, flow, damper, reheat).
4. `zone_pairs.build_zone_pair_index(limit: int = 5000) -> Dict[Tuple[str, str], ZonePair]`  
   - Reads `sqlite_store.list_series`, infers zone roots with `ZONE_RE`, matches roles via regex patterns in `ROLE_PATTERNS`, and populates `ZonePair` dataclasses that map history IDs to roles such as `space_temp`, `flow`, `damper`, `fan_cmd`, etc.  
   - Helpers `find_zone_pair` and `zone_pairs_as_dicts` expose the resulting metadata for API filters.
5. `_query_series_df(...)` (used in `zone_health`) – normalizes `sqlite_store.query_series` output to pandas DataFrames, converts mixed ISO timestamps to naive UTC, sorts, and returns columns renamed to the expected value column for analytics.

## 8. Assumptions and Conventions
- **Timestamp handling:** All history timestamps are stored in SQLite as UTC ISO strings (`ts TEXT`). `sqlite_store.query_series` returns whatever string Niagara sent (with or without fractional seconds/offset), and `zone_health`/`_query_series_df` parse them into naive UTC datetimes so analytics can align series without timezone drift.
- **Niagara naming:** `niagara_decode_name` removes hex escapes (`$20`, `$2d`), adds spaces between camelCase/digit boundaries, and normalizes repeated dashes; `niagara_canonical_name` lowercases the result and replaces non-alphanumeric characters with `_`, so all series share stable keys for storage/indexing and filtering (used by zone pairing and query filters).
- **MQTT payloads:** `mqtt_history_ingest.make_history_mqtt_client` expects JSON frames with `"messageType": "history"`, a non-empty `stationName`, optional `historyId` (falls back to `id`, `historyName`, or `name`), and a `historyData` list of objects containing `timestamp` strings (`2025-11-24 00:00:01.349-0700` style) plus numeric `value`. Status fields are optional.
- **Merge tolerances:** Comfort/flow/damper joins use pandas `merge_asof` with a 30-second tolerance; `/debug/comfort_zone_pair` exposes a query parameter to vary that window (30–300s by defaults), while `FlowTrackingConfig.merge_tolerance_seconds` is also 30s. This reflects the assumption that Niagara publishes roughly once a minute per point.
- **Retention and caps:** SQLite retention is driven by `AppConfig.db_retention_hours` (default 720 hours/30 days) with `_apply_retention` trimming older rows after each batch; the in-memory store keeps at most `_MAX_PER_SERIES = 1000` samples per series. `MqttHistoryClient.retention_hours` defaults to 24 hours.
- **Comfort assumptions:** Comfort scoring only considers data whose timestamp falls between `occupied_start` and `occupied_end`, and “within band” is defined as `abs(temp - setpoint) <= comfort_band_degF`. Reheat waste flags readings where reheat valve > 0 while space temp exceeds setpoint plus a 1°F deadband.
- **Overall score:** `compute_zone_health` weights comfort (×3), flow (×2), and takes inverse percentages for damper/reheat faults, averaging them into a single score whenever any of those metrics are available; missing data results in `overall_score=None` so endpoints can sort worst-first without assuming completeness.
