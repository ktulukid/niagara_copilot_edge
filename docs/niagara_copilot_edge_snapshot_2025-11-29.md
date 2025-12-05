# Niagara Copilot Edge Snapshot — 2025-11-27

## 1. Project Summary
- Niagara Copilot Edge subscribes to Niagara history data (primary MQTT `niagara/histories`, optional Haystack via `pyhaystack`, CSV exports, oBIX/servlet clients) and normalises records into `HistorySample` objects before storage.
- Incoming batches feed both the in-memory `store.history_store` (fast debug view) and the durable `store.sqlite_store.history_samples` table, with hourly retention driven by `AppConfig.db_retention_hours`.
- Canonicalisation helpers (`niagara_decode_name`, `niagara_canonical_name`) plus regex-driven rules group histories into zone/equipment pairings so analytics know which temperature, flow, damper, and reheat signals belong together.
- Analytics functions (`compute_zone_comfort`, `compute_flow_tracking`, `compute_zone_health`) derive comfort, flow tracking, damper sanity, reheat wasting, fan diagnostics, and an overall score from aligned pandas frames.
- FastAPI (`src.api.server`) exposes health, debug, summary, and Haystack test endpoints that query the SQLite store, run analytics, and return Pydantic models; the server also keeps the MQTT and optional Haystack clients live.
- Configuration (`src.config`) defines every layer—data source selection, comfort thresholds, MQTT connection, Haystack credentials, and SQLite retention—ensuring deployments remain site-specific yet consistent.

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
│   ├── haystack_client.py
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
- `src/analytics/comfort.py` defines `compute_zone_comfort(df, comfort_cfg)` which filters a merged setpoint/temp DataFrame to occupied hours, calculates deviation from `comfort_band_degF`, and returns samples/within-band percentage/mean error.
- `src/analytics/flow.py` exposes `FlowTrackingConfig` (column names, percent/absolute tolerances, merge seconds) and `compute_flow_tracking(df_flow, df_flow_sp, cfg)` that aligns flow vs setpoint via `merge_asof` and reports error statistics.
- `src/analytics/zone_pairs.py` infers equipment “zone roots” (VAV, FPB, AHU, etc.) with `ZONE_RE`, maps roles via `ROLE_PATTERNS`, and builds `ZonePair` dataclasses through `build_zone_pair_index`; helpers `find_zone_pair` and `zone_pairs_as_dicts` surface metadata for the API.
- `src/analytics/zone_health.py` orchestrates `_compute_comfort`, `_compute_flow_and_damper`, `_compute_reheat_waste`, `_compute_overall_score` and returns `ZoneHealthMetrics` via `compute_zone_health`; `zone_health_to_dict` serialises metrics for endpoints.
- `src/api/server.py` bootstraps config, SQLite, MQTT, and even an optional Haystack client, then wires FastAPI routes (`/health`, `/debug/*`, `/summary/*`, `/haystack/test/*`) to the stores and analytics modules, defining response models like `ZoneHealthMetricsModel`.
- `src/niagara_client/analytics_api.py` wraps the Niagara Analytics Web API payloads (`AnalyticsApiClient`, `AnalyticsResponse`, `AnalyticsResponseEnvelope`) used for node browsing and tree metadata if needed.
- `src/niagara_client/factory.py` declares the `HistoryClient` protocol, resolves Haystack credentials from `AppConfig`, and provides `make_history_client`/`make_haystack_client` factories that currently deliver `HaystackHistoryClient`.
- `src/niagara_client/haystack_client.py` is a thin `pyhaystack` wrapper with `HaystackHistoryClient.read_by_filter`, `his_read`, and helper `find_zone_temp_points` that normalise Haystack grids into plain dicts/tuples for API tests.
- `src/niagara_client/history_http_obix.py` reads Niagara oBIX historyQuery XML, parsing `<abstime>`/`<real>` nodes into pandas DataFrames sorted by timestamp.
- `src/niagara_client/mqtt_history_client.py` keeps an in-process rolling cache of JSON MQTT history records keyed by the comfort config’s `equip_column`, filtering by equip/time for in-memory usage.
- `src/niagara_client/mqtt_history_ingest.py` defines `HistorySample`, canonicalisation helpers, validation/parsing of `messageType=history` frames, and `make_history_mqtt_client` which emits batches to a callback (used by the FastAPI server to seed both stores).
- `src/niagara_client/niagara_csv_export_client.py` enumerates CSV links from `historyExports/<station>`, downloads them via authenticated `requests`, merges them with pandas, and filters by equipment/time before returning comfort-ready DataFrames.
- `src/niagara_client/niagara_servlet_client.py` and `history_http_obix.py` offer alternative HTTP history sources (Niagara servlet JSON and oBIX) that can feed the same analytics layers if wired into `make_history_client`.
- `src/store/history_store.py` maintains a per-(station,history) in-memory dict limited to `_MAX_PER_SERIES = 1000`, exposes `add_batch`, `clear`, and `get_recent` (JSON serialised) for debugging endpoints.
- `src/store/sqlite_store.py` hosts the durable SQLite store, initialises the `history_samples` table/indexes, `add_batch` (with canonical keys and UTC `ts`), `_apply_retention`, and query helpers `list_series`/`query_series`.
- `src/config.py` defines Pydantic models `AppConfig`, `DataSourceConfig`, `ComfortConfig`, `MqttConfig`, `MqttJsonStreamConfig`, `NiagaraCsvExportConfig`, and `HaystackConfig`, plus `load_config` that reads `config/config.yaml` and optionally prompts for MQTT overrides.
- `src/main.py` simply runs `uvicorn` pointing at `src.api.server:app` for local development.

## 3. Configuration Models
- `MqttConfig`  
  - `host: str = "localhost"` – broker hostname for `make_history_mqtt_client`.  
  - `port: int = 1883` – MQTT port (can be overridden via `MQTT_HOST`/`MQTT_PORT` env).  
  - `history_topic: str = "niagara/histories"` – topic subscribed for Niagara history frames.
- `NiagaraCsvExportConfig`  
  - `host: str` – IP/hostname of the Niagara station.  
  - `ord_path: str` – URI-encoded `file:^historyExports/<station>` path to enumerate CSVs.  
  - `username: str`/`password_env: str` – HTTP Basic credentials (password fetched from env).  
  - `insecure_tls: bool = True` – allow self-signed certs when connecting via `requests`.
- `MqttJsonStreamConfig`  
  - Connection details (`host`, `port`, `topic`, optional `username`, `client_id`, `tls`, `keepalive`).  
  - `password_env: Optional[str]` – env name holding the password if authentication is used.  
  - `retention_hours: int = 24` – how long `MqttHistoryClient` keeps messages before pruning.
- `HaystackConfig`  
  - `uri: str` – base Niagara/nHaystack URL (e.g., `http://172.20.40.22`).  
  - `username: str`/`password_env: str` – credentials for Haystack sessions.  
  - `project: str = "default"` – optional hint translated into `HaystackHistoryClient`.
- `DataSourceConfig`  
  - `type: Literal["niagara_csv_export","mqtt_json_stream","haystack"]` – selects which nested config is populated.  
  - Optional nested configs: `niagara_csv_export`, `mqtt_json_stream`, or `haystack`.
- `ComfortConfig`  
  - `occupied_start`/`occupied_end: str` – daily window for comfort scoring (e.g., `"07:00"`/`"18:00"`).  
  - Column names `setpoint_column`, `temp_column`, `timestamp_column`, `equip_column` – align pandas/JSON payloads with comfort analytics.  
  - `comfort_band_degF: float` – allowable deviation before marking a sample as out-of-band.
- `AppConfig`  
  - `site_name: str` – deployment identifier surfaced by `/health`.  
  - `data_source: DataSourceConfig` – picks the ingestion path plus optional Haystack metadata.  
  - `comfort: ComfortConfig`.  
  - `mqtt: MqttConfig`.  
  - `db_path: str = "data/history.sqlite"` – persisted SQLite file location.  
  - `db_retention_hours: int = 720` – rolling retention window enforced after each batch.  
  - `haystack: Optional[HaystackConfig] = None` – root-level defaults for Haystack credentials (overrides `data_source.haystack` if provided).
- `load_config(path="config/config.yaml")` – parses YAML into `AppConfig` and interactively prompts (if stdin available) for MQTT host/port overrides.

## 4. Data Flow: Ingestion → Storage → Analytics → API
- **Ingestion:**  
  - `make_history_mqtt_client` (in `src/niagara_client/mqtt_history_ingest.py`) subscribes to `history_topic`, validates the `messageType="history"` JSON path, decodes timestamps like `2025-11-24 00:00:01.349-0700`, normalises names via `niagara_decode_name/niagara_canonical_name`, and emits `List[HistorySample]`.  
  - `HaystackHistoryClient` (via `src/niagara_client/haystack_client.py` + `factory.make_history_client`) uses `NiagaraHaystackSession` to `read_by_filter` or `his_read` for historical data when `data_source.type == "haystack"` and powers test endpoints.  
  - Other helpers (`NiagaraCsvExportClient`, `NiagaraHistoryServletClient`, `NiagaraObixHistoryClient`, `MqttHistoryClient`) each understand their respective sources (CSV index HTML, servlet JSON, oBIX XML, or JSON stream) and produce pandas DataFrames shaped to `ComfortConfig` columns for analytics work.
- **Storage:** `_on_history_batch` in `src/api/server.py` feeds `store.history_store.add_batch` (per-series cache limited to `_MAX_PER_SERIES=1000`) and `store.sqlite_store.add_batch`. `sqlite_store.add_batch` canonicalises station/history keys, converts timestamps to UTC ISO, and inserts with `INSERT OR REPLACE`, enforcing `db_retention_hours` via `_apply_retention`.
- **Querying:**  
  - `sqlite_store.list_series(limit)` returns filtered `(station_key, history_key, station_name, history_id)` rows used by `zone_pairs.build_zone_pair_index`.  
  - `sqlite_store.query_series(station, history_id, start, end, limit)` filters by canonical keys, time window, and sorts by `ts`, giving JSON-ready rows that `_rows_to_dataframe` (FastAPI helper) converts into pandas for analytics.
- **Analytics:**  
  - `zone_pairs.build_zone_pair_index` scans series metadata, infers zone roots, and maps roles (space_temp, flow, damper, etc.) into `ZonePair`; `find_zone_pair`/`zone_pairs_as_dicts` expose this metadata.  
  - `compute_zone_comfort` merges temperature and setpoint frames, filters to occupied hours, and computes within-band percentages.  
  - `compute_flow_tracking` merges flow vs setpoint series with `FlowTrackingConfig.merge_tolerance_seconds`, calculates error CFM/%, and reports within-band coverage.  
  - `compute_zone_health` calls `_compute_comfort`, `_compute_flow_and_damper`, `_compute_reheat_waste`, `_compute_overall_score`, and emits `ZoneHealthMetrics` containing comfort, flow, damper, reheat, fan, and overall_score metrics that `zone_health_to_dict` feeds into API models.
- **API Exposure:** FastAPI routes in `src/api/server.py` fetch data from `sqlite_store`, run the analytics modules, and return Pydantic responses: debug endpoints (`/debug/recent_memory`, `/debug/zone_pairs`, `/debug/comfort_zone_pair`, `/debug/flow_tracking`), summaries (`/summary/zone_health`, `/summary/building_health`), and Haystack tests (`/haystack/test/zoneTemps`, `/haystack/test/history`). Each debug/summary function calls the relevant helper (`history_store.get_recent`, `zone_pairs_as_dicts`, `sqlite_store.query_series`, `compute_zone_comfort`, `compute_flow_tracking`, `compute_zone_health`).

## 5. Database Schema
- **Table `history_samples`** (`store.sqlite_store._init_schema`):  
  - `station_key TEXT NOT NULL`, `history_key TEXT NOT NULL` – canonical snake_case identifiers (from `niagara_canonical_name`).  
  - `station_name TEXT NOT NULL`, `history_id TEXT NOT NULL` – decoded, human-readable labels for APIs.  
  - `ts TEXT NOT NULL` – UTC ISO timestamp (`HistorySample.timestamp.astimezone(timezone.utc).isoformat()`).  
  - `status TEXT` – optional status/meta string.  
  - `value REAL NOT NULL` – numeric measurement.  
  - Primary key `(station_key, history_key, ts)` ensures idempotent overwrites per timestamp.  
- **Indexes:**  
  - `idx_history_samples_ts` on `ts` accelerates retention pruning and time-bound queries.  
  - `idx_history_samples_series` on `(station_key, history_key, ts)` speeds `query_series`.  
- `store.sqlite_store.add_batch` writes canonical rows (with `INSERT OR REPLACE`) and calls `_apply_retention` to delete rows older than `db_retention_hours`.  
- Readers: `sqlite_store.list_series` / `query_series` feed `zone_pairs`, `compute_zone_health`, summary endpoints, and debug utilities; Haystack tests bypass SQLite entirely, using the Haystack client directly.

## 6. API Endpoints
- `GET /health` – returns `HealthResponse(status="ok", site_name)`; used for readiness checks.  
- `GET /debug/recent_memory` – params `station`, `history_id`, `limit`; calls `history_store.get_recent`, returns `HistorySampleJson` entries (`stationName`, `historyId`, UTC `timestamp`, `status`, `value`).  
- `GET /debug/zone_pairs` – optional `station`, `zone` filters; iterates `zone_pairs_as_dicts` (list of zone metadata dicts) and returns `ZonePairResponse` objects listing detected history IDs for `space_temp`, `flow`, `damper`, `reheat`, `fan_cmd`, `fan_status`.  
- `GET /debug/comfort_zone_pair` – params `station`, `temp_history_id`, `sp_history_id`, `hours`, `merge_tolerance_seconds`; queries both histories via `sqlite_store.query_series`, merges them with pandas `merge_asof`, runs `compute_zone_comfort`, and returns `ComfortZonePairResponse` (history IDs, time window, `ComfortMetricsModel` with `samples`, `within_band_pct`, `mean_error_degF`).  
- `GET /debug/flow_tracking` – params `station`, `zone`, `hours`; finds the zone via `zone_pairs_as_dicts`/`find_zone_pair`, queries the flow (and optional flow setpoint) series, runs `compute_flow_tracking`, and returns `FlowTrackingResponse` (`FlowTrackingMetricsModel` includes `samples`, `within_band_pct`, `mean_error_cfm`, `mean_error_pct`).  
- `GET /summary/zone_health` – params `station`, `zone`, `hours`; looks up the zone pair, uses `compute_zone_health` to combine comfort, flow, damper, reheat, fan diagnostics, and overall score, and returns `ZoneHealthMetricsModel` (comfort/flow/damper/reheat/fan fields plus `status`, `reasons`, `overall_score`).  
- `GET /summary/building_health` – params `station`, `hours`; loops every zone pair for the station, collects `ZoneHealthMetricsModel` values, sorts worst-first (critical → ok → no_data, then by `overall_score`), and returns the list for dashboards.  
- `GET /haystack/test/zoneTemps` – optional `site_ref`; requires `_haystack_client`, calls `HaystackHistoryClient.find_zone_temp_points`, normalises Haystack types via `_normalize_for_json`, and returns `{"count", "points"}` for debugging.  
- `GET /haystack/test/history` – params `id`, `range`; requires `_haystack_client`, calls `HaystackHistoryClient.his_read`, normalises timestamps to ISO strings, and returns `{"id", "range", "samples": [{"ts","val"}...]}`.

## 7. Analytics Functions
1. `compute_zone_comfort(df: pandas.DataFrame, comfort_cfg: ComfortConfig) -> dict`  
   - `df`: merged temperature/setpoint rows renamed to the columns defined in `comfort_cfg`.  
   - `comfort_cfg`: defines occupied window, column names, and `comfort_band_degF`.  
   - Returns `{"samples": int, "within_band_pct": float | None, "mean_error_degF": float | None}` computed only over occupied-hour rows after aligning timestamps to `comfort_cfg.timestamp_column`.  
2. `compute_flow_tracking(df_flow: pandas.DataFrame, df_flow_sp: Optional[pandas.DataFrame], cfg: FlowTrackingConfig | None = None) -> dict`  
   - Aligns flow versus flow setpoint via pandas `merge_asof` within `cfg.merge_tolerance_seconds`, calculates `error_cfm`, `%` error, and `within_band` based on max of percent and optional absolute tolerances.  
   - Returns `{"samples", "within_band_pct", "mean_error_cfm", "mean_error_pct"}` used by `/debug/flow_tracking` and `compute_zone_health`.  
3. `build_zone_pair_index(limit: int = 5000) -> Dict[Tuple[str, str], ZonePair]` (and helpers)  
   - Reads `sqlite_store.list_series`, infers zone roots with `ZONE_RE`, assigns roles from `ROLE_PATTERNS`, and populates `ZonePair` dataclasses that map each role (space_temp, flow, damper, etc.) to the first matching history.  
   - `find_zone_pair(station_key, zone_root)` and `zone_pairs_as_dicts()` surface this metadata for debug/summary endpoints.  
4. `compute_zone_health(station: str, zone_root: str, zone_info: Dict[str, Any], comfort_cfg: ComfortConfig, start: Optional[datetime]=None, end: Optional[datetime]=None) -> ZoneHealthMetrics`  
   - Orchestrates `_compute_comfort`, `_compute_flow_and_damper`, `_compute_reheat_waste`, and `_compute_overall_score`, each querying series via `_query_series_df` (normalises ISO timestamps to naive UTC).  
   - Returns metrics covering comfort samples, flow stats, damper high-open/closed-high-flow rates, reheat waste, fan discrepancies, and a weighted `overall_score` used by `/summary/*` endpoints.  
5. `_query_series_df(...)` (internal to `zone_health`)  
   - Calls `sqlite_store.query_series`, parses mixed ISO timestamp strings into UTC-naive `datetime`, renames columns, and sorts to feed pandas merges across comfort/flow/damper/reheat streams.

## 8. Assumptions and Conventions
- **Timezone handling:** `sqlite_store` stores UTC ISO strings (`ts`), `zone_health._query_series_df` parses them with `utc=True` and drops tz info so pandas alignments operate on naive UTC times; `compute_zone_comfort` and flow tracking compare timestamps truncated to the configured occupied window.  
- **Timestamp formats:** MQTT/haystack samples expect Niagara formats like `2025-11-24 00:00:01.349-0700`, CSV/servlet payloads rely on full ISO strings, and `sqlite_store.query_series` retains the raw string before downstream parsers normalise.  
- **MQTT payload format:** `niagara_client.mqtt_history_ingest._validate_history_frame` demands `messageType="history"`, non-empty `stationName`, optional `historyId` (falls back to `id`/`historyName`/`name`), and `historyData` rows each with `timestamp` and `value`; `status` is optional. Each validated message becomes `HistorySample` with decoded station/history labels.  
- **Niagara naming conventions:** `niagara_decode_name` removes `$20/$2d` escapes, adds spaces between camelCase/digit transitions, and collapses duplicates; `niagara_canonical_name` lowercases and replaces non-alphanumeric sequences with `_`, ensuring stable `station_key`/`history_key` for SQLite and zone pairing.  
- **Haystack conventions:** `HaystackHistoryClient` floods `pyhaystack` responses; `read_by_filter` preserves plain dicts (coercing `id` to `@<ref>`), while `his_read` accepts IDs with or without `@` and range strings like `"today"` or `"2025-11-27,2025-11-28"`; API tests normalise values to strings/floats for JSON.  
- **Merge tolerances:** Comfort, flow, and damper joins use pandas `merge_asof` with 30-second tolerance (configurable via `/debug/comfort_zone_pair` `merge_tolerance_seconds` or `FlowTrackingConfig`).  
- **Retention and caps:** SQLite retention obeys `AppConfig.db_retention_hours` (default 720h, purged after each batch), in-memory history store caps at `_MAX_PER_SERIES = 1000`, and `MqttHistoryClient.retention_hours` defaults to 24h.  
- **Comfort/reheat thresholds:** Comfort scoring only considers data within `[occupied_start, occupied_end]`, within-band defined as `abs(temp-setpoint) <= comfort_band_degF`, damper sanity checks compare `damper` vs `flow` extremes, and reheat waste flags reheat > 0 when space temp exceeds setpoint + 1°F deadband.  
- **Overall scoring:** `compute_zone_health._compute_overall_score` weights comfort (×3), flow (×2), and penalises damper/reheat faults via `(100 - bad_pct)` averages, returning `overall_score` only when any component has data; APIs sort lower scores first but place `None` at the bottom.
