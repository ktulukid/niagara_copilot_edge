# Niagara Copilot Edge Snapshot (2025-11-27)

## 1. Project Summary
- Ingests Niagara telemetry from MQTT JSON streams (and optionally CSV exports or oBIX history queries) into typed `HistorySample` objects so the service can reason about temperature, setpoint, and status data.
- Persists every sample twice: first into an in-memory cache for fast debug endpoints (`src/store/history_store.py`) and then into a rolling SQLite history (`src/store/sqlite_store.py`) where retention and de-duplication are enforced.
- Runs a FastAPI server (`src/api/server.py`) that exposes comfort analytics, debug queries, series discovery, and raw series reads as well as newer zone/flow insights so operators can check current control performance.
- Computes comfort metrics through `compute_zone_comfort` in `src/analytics/comfort.py`, which compares configured temperature columns against a comfort band during occupied hours.
- Builds VAV zone metadata (`src/analytics/zone_pairs.py`) from SQLite series metadata and surfaces flow tracking analytics (`src/analytics/flow.py`) that align box flow and flow setpoint series with merge-asof tolerances.
- Uses YAML-driven configuration (`config/config.yaml` + `src/config.py`) for site metadata, data sources, comfort column mapping, MQTT broker details, and SQLite retention so deployments stay consistent while still allowing MQTT overrides during startup.

## 2. Directory and Module Map
src/
|- api/
|  `- server.py - FastAPI app, MQTT ingestion startup (`start_mqtt_ingestion`), in-memory + SQLite integration, and endpoints for comfort, debug/series data, zone discovery, and flow tracking.
|- analytics/
|  |- comfort.py - `compute_zone_comfort(df, comfort_cfg)` filters pandas rows to occupied hours, evaluates the comfort band, and returns sample count, within-band percentage, and mean error.
|  |- flow.py - `FlowTrackingConfig` plus `compute_flow_tracking(df_flow, df_flow_sp, cfg)` align flow vs flow setpoint histories with `merge_asof`, calculate percent/CFM errors, and report how many samples stay within tolerance.
|  `- zone_pairs.py - Heuristic helpers (`build_zone_pair_index`, `find_zone_pair`, `zone_pairs_as_dicts`) scan `sqlite_store.list_series()` to map canonical station keys and zone roots to their captured temperature/airflow/damper/reheat histories.
|- niagara_client/
|  |- analytics_api.py - Niagara Analytics Web API client plus pydantic envelopes for node navigation.
|  |- factory.py - History client factory stub (currently returns `None`) so the API knows when CSV/HTTP clients are available.
|  |- history_http_obix.py - oBIX historyQuery reader that returns DataFrames with `timestamp`/`value` columns if needed.
|  |- mqtt_history_client.py - Keeps recent MQTT JSON records in memory per `MqttJsonStreamConfig` so other code can request `get_zone_history` without hitting the broker.
|  |- mqtt_history_ingest.py - MQTT subscription helper that validates `messageType = 'history'`, decodes Niagara names, builds `HistorySample`, and feeds batches into `handle_history_batch`.
|  |- niagara_csv_export_client.py - Crawls Niagara HistoryExports HTML to download CSVs, concatenates them via pandas, and filters equip/time ranges using `ComfortConfig` column names.
|  `- niagara_servlet_client.py - Placeholder showing how Niagara servlet JSON could be mapped to the configured comfort column names.
|- store/
|  |- history_store.py - In-memory cache keyed by canonical station/history, trimmed to `_MAX_PER_SERIES = 1000`, returning JSON-ready dicts used by `/debug/histories` and `/debug/comfort`.
|  `- sqlite_store.py - SQLite persistence centered on `history_samples`, retention enforcement, `list_series`, and `query_series` helpers for the API and analytics.
|- config.py - Pydantic models (`AppConfig`, `DataSourceConfig`, `ComfortConfig`, etc.) plus `load_config` that reads `config/config.yaml`, prompts for MQTT overrides, and returns the typed object.
`- main.py - `uvicorn.run("src.api.server:app", ...)` entrypoint for running the FastAPI service.

## 3. Configuration Models
- `DataSourceType` - Literal tag `"niagara_csv_export"` or `"mqtt_json_stream"` driving which client is enabled.
- `NiagaraCsvExportConfig`
  - `host: str` - Niagara host used to build `/file/<station>/historyExports/` URLs.
  - `ord_path: str` - Expected form `file:^historyExports/<station>` used to infer the station name.
  - `username: str` - HTTP user for Niagara exports.
  - `password_env: str` - Environment variable name holding the password (defaults to `"NIAGARA_PASSWORD"`).
  - `insecure_tls: bool` - `True` allows self-signed Niagara certificates.
- `MqttJsonStreamConfig`
  - `host: str`, `port: int = 8883` - MQTT broker host/port for Niagara history topics.
  - `topic: str` - MQTT topic (usually `"niagara/histories"`) holding history JSON.
  - `username: Optional[str]`, `password_env: Optional[str]` - Optional MQTT auth; if username is set, `password_env` must point to an environment variable containing the password.
  - `tls: bool = True` - Whether TLS is enabled for MQTT/TLS streams.
  - `client_id: Optional[str]`, `keepalive: int = 60` - Client metadata.
  - `retention_hours: int = 24` - How long `MqttHistoryClient` keeps recent records for in-memory filtering.
- `DataSourceConfig`
  - `type: DataSourceType` - Switch between CSV export or MQTT ingestion modes.
  - `niagara_csv_export: Optional[NiagaraCsvExportConfig]`, `mqtt_json_stream: Optional[MqttJsonStreamConfig]` - Source-specific configs; only the chosen `type` must be non-null.
- `ComfortConfig`
  - `occupied_start: str`, `occupied_end: str` - Occupancy window in `HH:MM` used by `compute_zone_comfort`.
  - `setpoint_column`, `temp_column`, `timestamp_column`, `equip_column: str` - Column names expected on any historical DataFrame so the analytics function can align data.
  - `comfort_band_degF: float` - Tolerance band around the setpoint, used to compute `% within band`.
- `FlowTrackingConfig`
  - `timestamp_column: str = "timestamp"`, `value_column: str = "value"` - Columns from flow and flow setpoint series that hold the UTC timestamp and numeric measurement.
  - `pct_tolerance: float = 0.1` - Percent-of-setpoint tolerance (e.g., +/-10%) for declaring "within band".
  - `abs_cfm_tolerance: Optional[float] = None` - Optional absolute airflow tolerance in CFM; the effective tolerance becomes `max(abs_cfm_tolerance, pct_tolerance * setpoint)` when provided.
  - `merge_tolerance_seconds: int = 30` - Temporal tolerance for `pd.merge_asof` so slightly offset samples still align.
- `MqttConfig`
  - `host: str`, `port: int = 1883`, `history_topic: str = "niagara/histories"` - Broker details used by `start_mqtt_ingestion` for the background subscriber.
- `AppConfig`
  - `site_name: str` - Display label returned by the `/comfort/zone` endpoint.
  - `data_source: DataSourceConfig` - Entire source configuration described above.
  - `comfort: ComfortConfig` - Comfort/column mapping.
  - `mqtt: MqttConfig` - MQTT ingestion overrides.
  - `db_path: str = "data/history.sqlite"` - SQLite database file location.
  - `db_retention_hours: int = 24 * 30` - Rolling retention window for persisted samples.
  - `load_config(path)` - Reads YAML from `config/config.yaml`, applies interactive overrides for MQTT host/port when stdin is available, and returns the filled `AppConfig`.

## 4. Data Flow: Ingestion -> Storage -> Analytics -> API
- **Ingestion**
  - MQTT history messages arrive on `niagara/histories` and are parsed inside `make_history_mqtt_client` (`src/niagara_client/mqtt_history_ingest.py`). Each JSON frame is validated (`_validate_history_frame`), timestamps are parsed (`_parse_timestamp`), Niagara names are decoded/canonicalized (`niagara_decode_name`, `_decode_history_label`), and `HistorySample` dataclasses are emitted to the `handle_history_batch` callback in `src/api/server.py`.
  - Optional CSV ingestion (`NiagaraCsvExportClient`) crawls the `historyExports/<station>` directory over HTTPS, downloads CSV files, concatenates them via pandas, parses timestamps, and filters rows by equipment and `[start,end]` using the configured timestamp/equipment columns.
  - Additional clients (`NiagaraHistoryServletClient`, `NiagaraObixHistoryClient`, `MqttHistoryClient`) illustrate alternative sources that produce pandas DataFrames with consistent column naming; the FastAPI comfort endpoint currently expects a `get_zone_history` method to be provided by `make_history_client`.
- **Storage**
  - `HistorySample` records pass through `history_store.add_batch`, where they are canonicalized via `niagara_canonical_name`, kept per-series, capped at `_MAX_PER_SERIES = 1000`, and converted to ISO timestamps for JSON-ready debug queries.
  - The same batch is persisted to SQLite through `sqlite_store.add_batch`: each row stores canonical keys, human-readable names, ISO-8601 UTC timestamps, status, and value, while `INSERT OR REPLACE` prevents duplicates. `_apply_retention` removes rows older than `db_retention_hours`, and indexes on `ts` and `(station_key, history_key, ts)` keep queries fast.
  - `zone_pairs.build_zone_pair_index` and related helpers scan `sqlite_store.list_series()` metadata to fingerprint VAV roots and their key histories, enabling the `/zones` endpoints without needing more data ingestion.
- **Analytics**
  - Both live MQTT data and persisted SQLite rows are mapped to pandas DataFrames with columns named by `ComfortConfig`. `compute_zone_comfort(df, comfort_cfg)` filters rows inside the occupied window, computes the error between `temp_column` and `setpoint_column`, and returns sample count, percent within the comfort band, and mean error.
  - Flow tracking (`compute_flow_tracking`) reads box flow and flow setpoint series, aligns them with `pd.merge_asof` inside `FlowTrackingConfig.merge_tolerance_seconds`, and reports CFM/percent errors plus the share of samples within band.
- **API**
  - The FastAPI endpoints fetch DataFrames either from the external history client (`/comfort/zone`), from local stores (`/debug/comfort`, `/debug/comfort_zone_pair`, `/series`, `/series/data`), or from the newly added zone/flow metadata, returning JSON that focuses on site/equipment identity plus computed metrics or raw samples.

## 5. Database Schema
- Table `history_samples`
  - Columns
    - `station_key TEXT`: canonical snake_case key from `niagara_canonical_name`.
    - `history_key TEXT`: canonicalized history identifier.
    - `station_name TEXT`, `history_id TEXT`: human-readable labels from decoded Niagara names.
    - `ts TEXT`: ISO-8601 UTC timestamp of the sample (written by `add_batch` using `timestamp.astimezone(timezone.utc).isoformat()`).
    - `status TEXT`: optional status string from the incoming MQTT payload.
    - `value REAL`: the numeric measured value stored as float.
  - Primary key `(station_key, history_key, ts)` ensures each (series,timestamp) is unique.
  - Indexes `idx_history_samples_ts` and `idx_history_samples_series` support time-range scans and per-series lookups.
- Writers/Readers
  - `sqlite_store.add_batch` inserts/upserts rows and triggers `_apply_retention` to delete older samples based on `db_retention_hours`.
  - `sqlite_store.list_series` reads the distinct station/history pairs (used by `/series`).
  - `sqlite_store.query_series` filters rows by canonical station/history, optional start/end bounds, and limit before returning JSON-friendly dictionaries for `/series/data`, `/debug/comfort_zone_pair`, and the zone flow tracking endpoint.

## 6. API Endpoints
- `GET /comfort/zone`
  - Calls `_history_client.get_zone_history(equip, start, end)` and `compute_zone_comfort`.
  - Returns `ComfortResponse` (`site`, `equip`, `start`, `end`, `samples`, `within_band_pct`, `mean_error_degF`). Throws 503 if `_history_client` is `None`.
- `GET /debug/histories`
  - Calls `history_store.get_recent(station, history_id, limit)` on the in-memory store.
  - Returns filters plus count and list of JSON-ready samples (`stationName`, `historyId`, `timestamp`, `status`, `value`).
- `GET /series`
  - Calls `sqlite_store.list_series(limit)` over SQLite.
  - Returns `limit`, `count`, and `items` (each with `station_key`, `history_key`, `stationName`, `historyId`).
- `GET /series/data`
  - Calls `sqlite_store.query_series(station, history_id, start, end, limit)` with ISO-converted bounds.
  - Returns query metadata plus `samples` list with timestamp/value data and `status`.
- `GET /debug/comfort_zone_pair`
  - Reads two series (`tempHistoryId`, `spHistoryId`) via `sqlite_store.query_series`, converts to pandas DataFrames, merges them through `pd.merge_asof` (30s tolerance), remaps columns using `ComfortConfig`, and calls `compute_zone_comfort`.
  - Returns metadata plus `analytics` (comfort metrics).
- `GET /debug/comfort`
  - Reads recent in-memory samples via `history_store.get_recent`, maps MQTT fields to the configured columns, and calls `compute_zone_comfort` with a temporary setpoint.
  - Returns filters, the temporary setpoint used, and the `analytics` result.
- `GET /debug/zone_pairs`
  - Calls `zone_pairs_as_dicts()` and optionally filters by canonical `station_key` or normalized `zone_root` to inspect the discovered temperature/airflow/damper/reheat mappings.
  - Returns `ZonePairResponse` objects so clients can see which Niagara histories were classified into each VAV role (some entries may have null roles).
- `GET /zones`
  - Calls `zone_pairs_as_dicts(limit)` to build a best-effort index of VAV zone roots and their temperature/airflow/aux histories without loading raw samples.
  - Returns an array of `ZonePairResponse` objects with canonical `station_key`, `zone_root`, and discovered history IDs.
- `GET /zones/{station_key}/{zone_root}`
  - Calls `find_zone_pair(niagara_canonical_name(station_key), normalized_zone_root)` to locate the matching `ZonePair`.
  - Returns `ZonePairResponse` with the resolved `station_name`, `zone_root`, and available history IDs.
- `GET /zones/{station_key}/{zone_root}/flow_tracking`
  - Queries SQLite for the pair's flow and flow setpoint series (`zone.flow`, `zone.flow_sp`) with optional `start`, `end`, and `limit` filters.
  - Passes the resulting DataFrames to `compute_flow_tracking(df_flow, df_flow_sp, cfg)` where `cfg` reflects the defaults or optional overrides (`pct_tolerance`, `abs_cfm_tolerance`, `merge_tolerance_seconds`).
  - Returns `FlowTrackingResponse` with the zone metadata, filter bounds, and the `metrics` dictionary that includes samples, within-band %, CFM error, and percent error.

## 7. Analytics Functions
- `compute_zone_comfort(df: pandas.DataFrame, comfort_cfg: ComfortConfig) -> dict`
  - `df` must contain columns named by `comfort_cfg.timestamp_column`, `.temp_column`, `.setpoint_column`, and `.equip_column`; each row represents one sample for a single zone/equipment.
  - Filters samples to the configured occupied window (`occupied_start`, `occupied_end`), computes `error = temp_column - setpoint_column`, and measures `within_band_pct` relative to `comfort_band_degF`.
  - Returns `{"samples": int, "within_band_pct": float | None, "mean_error_degF": float | None}`; `None` values indicate no occupied samples.
  - Used by `/comfort/zone`, `/debug/comfort_zone_pair`, and `/debug/comfort` to surface comfort performance statistics.
- `compute_flow_tracking(df_flow: pandas.DataFrame, df_flow_sp: Optional[pandas.DataFrame], cfg: FlowTrackingConfig) -> dict`
  - `df_flow` and `df_flow_sp` must share `timestamp`/`value` columns (the names can be overridden via `FlowTrackingConfig`).
  - Aligns the two DataFrames with `pd.merge_asof` using `cfg.merge_tolerance_seconds`, drops rows without a setpoint, computes `error_cfm`, `error_pct`, and determines if each sample is within `max(abs_cfm_tolerance, pct_tolerance * flow_sp)`.
  - Returns `{"samples": int, "within_band_pct": float | None, "mean_error_cfm": float | None, "mean_error_pct": float | None}` where percentages are expressed as real percentages.
  - Used by `/zones/{station_key}/{zone_root}/flow_tracking` to surface airflow tracking health.

## 8. Assumptions and Conventions
- **Timezone/timestamp handling:** MQTT timestamps are parsed by `_parse_timestamp` (Niagara format with offsets) and stored in SQLite as UTC ISO strings. `query_series` converts `start`/`end` bounds to UTC before filtering. When pandas filters by timestamps, it uses the configured timestamp column (often naive `datetime` parsed from ISO strings).
- **MQTT payload format:** `make_history_mqtt_client` expects JSON objects with `messageType = "history"`, `stationName`, optional `historyId`/`id`/`historyName` fallback, and `historyData` rows each containing `timestamp`, `value`, and optional `status`. Invalid frames are ignored and logged.
- **Niagara naming conventions:** `niagara_decode_name` expands hex escapes, underscores, and camelCase transitions into human-readable labels (e.g., `Zone$2d1` -> `Zone-1`). `niagara_canonical_name` lowercases, replaces non-alphanumerics with `_`, and trims to derive consistent keys for storage. `_decode_history_label` drops redundant leading station segments when deriving `history_id`.
- **Zone root normalization:** `zone_pairs._infer_zone_root` lowers the history label, collapses separators to hyphens, and trims duplicates so `/zones/{station_key}/{zone_root}` can accept a canonical hyphenated root irrespective of the original formatting.
- **Retention and caching:** `history_store` keeps at most 1000 samples per (station, history) pair and sorts ascending timestamps before returning results. `sqlite_store` enforces `db_retention_hours` (default 720) via `_apply_retention`.
- **Merge and tolerance heuristics:** `/debug/comfort_zone_pair` and `/zones/.../flow_tracking` both rely on `pd.merge_asof` with a 30-second `merge_tolerance_seconds` window to align slightly offset series while still measuring within-band percentages.
- **Comfort assumptions:** `comfort_band_degF` defines the acceptable error bound, and only samples within the `[occupied_start, occupied_end]` window are evaluated. `setpoint_column`, `temp_column`, and `equip_column` must align across CSV/JSON sources for consistent analytics.
