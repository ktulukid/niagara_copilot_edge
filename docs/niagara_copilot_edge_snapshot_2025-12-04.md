# Niagara Copilot Edge Snapshot (2025-12-04)

## 1. Project Summary
- Ingests Niagara history frames published over MQTT (or alternately read via CSV export/servlet+Haystack clients) and normalizes every event into `HistorySample` dataclasses that carry station/history IDs, timestamps, values, statuses, and optional equipment/floor/point/unit/tags metadata.
- Persists those samples in a durable `history_samples` SQLite cache while also keeping an in-memory history store for quick debugging; retention is driven by `AppConfig.db_retention_hours` and each series retains the most recent 1,000 entries in memory.
- Builds equipment‑centric zone rolls via `zone_pairs_as_dicts`, which uses tag/name heuristics from `analytics/role_rules.py` to assign roles such as `space_temp`, `flow_sp`, `fan_cmd`, etc., enabling downstream comfort/flow/RTU analytics.
- Exposes FastAPI endpoints for health, debugging (recent memory, metadata, comfort/flow probes), summaries (zone index/health/building/RTU), and Haystack helper calls, with responses shaped by dedicated Pydantic models.
- Drives behavior through YAML/Pydantic configuration (`AppConfig`, `DataSourceConfig`, `ComfortConfig`, etc.) loaded by `load_config(config/config.yaml)` with interactive MQTT overrides when stdin is available.

## 2. Directory and Module Map
- `src/`
  - `analytics/`
    - `comfort.py`: Defines `_parse_time` and `compute_zone_comfort(df, comfort_cfg)` to filter occupied hours, compute error vs. setpoint, and return sample counts/within-band percentages/mean offsets for a zone.
    - `flow.py`: Declares `FlowTrackingConfig` and `compute_flow_tracking(df_flow, df_flow_sp, cfg)` which merges flow vs. setpoint series with a tolerance window and reports CFM errors and percentages.
    - `role_rules.py`: Loads rule specs from `config/role_rules.json` (fallback to `_DEFAULT_RULES_SPEC`) via `RoleRule` dataclass and exposes `infer_role(label, tags)` so zone pairing can map point names/tags to roles.
    - `zone_pairs.py`: Houses `ZonePair` metadata, `build_zone_pair_index(limit)` that groups `sqlite_store.list_series()` rows by equipment/floor, and helpers (`zone_pairs_as_dicts`, `find_zone_pair`) used by every summary endpoint.
    - `zone_health.py`: Implements `ZoneHealthMetrics`, `_query_series_df`, `_compute_*` helpers, `compute_zone_health(...)` to gather Pandas views of every role, score comfort/flow/damper/reheat, and derive status/reasons, plus `zone_health_to_dict`.
    - `rtu.py`: Defines dataclasses for fan/cooling/discharge metrics, helper functions for binary cycle counting and discharge tracking, and `compute_rtu_health(...)` with `rtu_health_to_dict`.
  - `api/`
    - `server.py`: Configures FastAPI, loads `_config`, initializes `sqlite_store`/MQTT/Haystack clients, declares shared Pydantic models (`ComfortMetricsModel`, `ZoneHealthMetricsModel`, etc.), and implements every health/debug/summary/Haystack endpoint while wiring analytics helpers.
  - `niagara_client/`
    - `analytics_api.py`: Thin `AnalyticsApiClient` that POSTs to Niagara Analytics Web API, parses `AnalyticsResponseEnvelope`, and exposes `get_node(node)` for integrating older analytics nodes.
    - `base.py`: Placeholder module (currently empty) reserved for shared history-client abstractions.
    - `factory.py`: Provides `HistoryClient` protocol, resolves Haystack config via `_resolve_haystack_config`, and exposes `make_history_client()`/`make_haystack_client()` that route by `data_source.type`.
    - `haystack_client.py`: Wraps `NiagaraHaystackSession` from `pyhaystack`, exposes `read_by_filter()` and `his_read()` helpers, plus `find_zone_temp_points()` for test endpoints.
    - `history_http_obix.py`: `NiagaraObixHistoryClient` that calls the `historyQuery` oBIX endpoint, parses XML, and returns pandas DataFrames.
    - `mqtt_history_client.py`: `MqttHistoryClient` that subscribes to a JSON MQTT stream configured via `MqttJsonStreamConfig`, caches parsed payloads with `_parsed_timestamp`, and filters them on demand for comfort analytics.
    - `mqtt_history_ingest.py`: Defines `HistorySample`, canonicalization helpers (`niagara_decode_name`, `niagara_canonical_name`), frame validation/decoding, `_on_mqtt_message`, and `make_history_mqtt_client(cfg)` that feeds `history_store`/`sqlite_store`.
    - `niagara_csv_export_client.py`: `NiagaraCsvExportClient` that lists CSV files from `historyExports`, downloads and concatenates them, then filters by equipment/time per `ComfortConfig`.
    - `niagara_servlet_client.py`: `NiagaraHistoryServletClient` that calls a user-provided servlet (`/niagaraCopilot`), expects JSON `historyData`, and exposes `get_zone_history`.
  - `store/`
    - `history_store.py`: In-memory store with `_MAX_PER_SERIES=1000`, `add_batch`, trimming logic, and `get_recent()` used by debug endpoints to expose the latest HistorySamples.
    - `sqlite_store.py`: Manages SQLite persistence (`init`, `_init_schema`, `_prune_old_rows`, `add_batch`, `list_series`, `query_series`), converts datetimes to UTC ISO strings, and keeps `_series_meta` for metadata enrichment.
  - `config.py`: Houses all Pydantic configuration models (`AppConfig`, `DataSourceConfig`, `ComfortConfig`, `NiagaraCsvExportConfig`, `MqttConfig`, `MqttJsonStreamConfig`, `HaystackConfig`) plus `load_config()` that reads `config/config.yaml` and prompts on stdin.
  - `main.py`: Launches `src.api.server:app` with `uvicorn`, enabling automatic reload for development runs.

## 3. Configuration Models
- **`MqttConfig`**
  - `host` (str, default `"localhost"`): MQTT broker host for ingestion clients.
  - `port` (int, default `1883`): MQTT broker port.
  - `history_topic` (str, default `"niagara/histories"`): Topic `make_history_mqtt_client` subscribes to.
- **`NiagaraCsvExportConfig`**
  - `host` (str): Niagara host serving `/file` exports (e.g., `"172.20.40.22"`).
  - `ord_path` (str): Ord path like `"file:%5EhistoryExports/AmsShop"` used to derive station directory.
  - `username` (str): HTTP Basic user for downloads.
  - `password_env` (str, default `"NIAGARA_PASSWORD"`): Environment variable pointing to the password.
  - `insecure_tls` (bool, default `True`): Allow self-signed certs when `True`.
- **`MqttJsonStreamConfig`**
  - `host` (str), `port` (int, default `8883`), `topic` (str): MQTT connection parameters for JSON streams.
  - `username`/`password_env` (optional str): Credentials for secured brokers.
  - `tls` (bool, default `True`): Whether to enable TLS.
  - `client_id` (optional str), `keepalive` (int, default `60`): MQTT client controls.
  - `retention_hours` (int, default `24`): How far back `MqttHistoryClient` keeps parsed records in memory.
- **`HaystackConfig`**
  - `uri`/`username`/`password_env`: Connection info for Niagara+nHaystack (password read from env).
  - `project` (str, default `"default"`): Optional hint for multi-project deployments.
- **`DataSourceConfig`**
  - `type` (Literal): `niagara_csv_export`, `mqtt_json_stream`, or `haystack`, dictating which client is active.
  - `niagara_csv_export`, `mqtt_json_stream`, `haystack` (optional): Sub-configs; only the relevant one is required.
- **`ComfortConfig`**
  - `occupied_start`/`occupied_end` (str `"HH:MM"`): Occupied window for comfort scoring.
  - `setpoint_column`, `temp_column`, `timestamp_column`, `equip_column` (str): Column names expected in incoming records.
  - `comfort_band_degF` (float): Band in °F around setpoint considered “comfort”.
- **`AppConfig`**
  - `site_name` (str): Used in health/status responses.
  - `data_source` (`DataSourceConfig`): Drives ingestion client selection.
  - `comfort` (`ComfortConfig`): Shared by analytics and MQTT clients.
  - `mqtt` (`MqttConfig`, default constructed): MQTT host/topic for `make_history_mqtt_client`.
  - `db_path` (str, default `"data/history.sqlite"`): SQLite file path.
  - `db_retention_hours` (int, default `720`): SQLite pruning window (30 days).
  - `haystack` (`HaystackConfig`, optional): Global Haystack defaults (prefers user-provided value over `data_source.haystack`).
- `load_config(path)` loads the YAML file, parses it into `AppConfig`, and prompts (if stdin is available) for MQTT host/port overrides before returning the model.

## 4. Data Flow: Ingestion → Storage → Analytics → API
- **Ingestion:** `make_history_mqtt_client(cfg)` subscribes to `cfg.mqtt.history_topic`, receives JSON frames, and routes each valid frame through `decode_history_frame()` so every row becomes a `HistorySample` (fields: `station_name`, `history_id`, `timestamp`, `value`, optional `status`, plus metadata `equipment`, `floor`, `point_name`, `unit`, `tags`). `_validate_history_frame` enforces `messageType == "history"`, requires `point` metadata, and expects `historyData` rows with `timestamp` and `value`. Alternative ingestion helpers (`NiagaraCsvExportClient`, `NiagaraHistoryServletClient`, `HaystackHistoryClient`, `MqttHistoryClient`) offer CSV/servlet/Haystack access paths that align with the same comfort config.
- **Storage:** Every `HistorySample` written by `_on_mqtt_message` is fed into `history_store.add_batch` (in-memory per-series cache keyed by canonical names) and `sqlite_store.add_batch` (persisting rows in `history_samples` while updating `_series_meta` for equipment/floor/point/unit/tags). `sqlite_store` enforces retention via `_prune_old_rows` using `AppConfig.db_retention_hours`.
- **Query:** `sqlite_store.list_series` returns distinct `(station, history_id)` pairs with metadata from `_series_meta`; `sqlite_store.query_series(station, history_id, start, end)` returns ordered rows with `ts` (UTC ISO string), `value`, and `status`. Debug endpoints (`/debug/recent_memory`, `/debug/series_meta`) and analytics helpers rely on these functions, while Pandas-based helpers (`_rows_to_dataframe`, `_query_series_df`) convert the ISO timestamps back to naive UTC datetimes.
- **Analytics & API:** `zone_pairs_as_dicts()` groups series by equipment/floor and uses `infer_role()` to assign roles such as `space_temp`, `flow`, `fan_cmd`, etc. `compute_zone_comfort`, `compute_flow_tracking`, `compute_zone_health`, and `compute_rtu_health` consume the queried DataFrames to produce metrics. FastAPI summary endpoints (`/summary/zone_index`, `/summary/zone_health`, `/summary/building_health`, `/summary/rtu_health`) call these analytics functions, wrap results in Pydantic models (e.g., `ZoneHealthMetricsModel`), and return JSON. Debug endpoints (`/debug/comfort_zone_pair`, `/debug/flow_tracking`) re-run comfort/flow calculations with merge tolerances and user-provided history IDs.

## 5. Database Schema
- Table `history_samples` created in `sqlite_store._init_schema()` has:
  - `id INTEGER PRIMARY KEY AUTOINCREMENT`: surrogate row key.
  - `station TEXT NOT NULL`: raw Niagara station name from `HistorySample.station_name`.
  - `history_id TEXT NOT NULL`: raw point identifier (e.g., `/AmsShop/Vav1_01$20SpaceTemperature`).
  - `ts_utc TEXT NOT NULL`: UTC ISO-8601 string derived from `HistorySample.timestamp` via `_to_utc_iso`.
  - `value REAL NOT NULL`: numeric reading (`float(history_sample.value)`).
  - `status TEXT`: optional status string forwarded from the MQTT payload.
- Index `idx_history_samples_station_hist_ts` covers `(station, history_id, ts_utc)` to optimize time-range queries by `query_series`.
- `sqlite_store._series_meta` keeps the latest equipment/floor/point/unit/tags seen per `(station, history_id)` so `list_series` can annotate metadata without additional joins.
- Writes happen exclusively through `sqlite_store.add_batch` (invoked by MQTT ingestion) and pruning occurs through `_prune_old_rows` when add_batch runs; reads happen through `list_series` (used in zone pairing and debug metadata) and `query_series` (used by comfort/flow/RTU analytics and API endpoints).

## 6. API Endpoints
- `GET /health`: returns `HealthResponse(status="ok", site_name=_config.site_name)` for service liveness.
- `GET /debug/recent_memory`: queries `history_store.get_recent(...)`, converts samples to `HistorySampleJson`, and returns the most recent `limit` cached in memory for a station/history pair.
- `GET /debug/series_meta`: calls `sqlite_store.list_series(limit)`, optionally filters by `station`, and returns `{count, rows}` of metadata-enriched pairs.
- `GET /debug/zone_pairs`: flattens `zone_pairs_as_dicts()` into `ZonePairResponse` objects so each role (space temp, flow, fan, valves, etc.) is visible along with equipment/floor display names.
- `GET /debug/comfort_zone_pair`: fetches two histories via `sqlite_store.query_series`, merges them with `pandas.merge_asof` using `merge_tolerance_seconds` (default 30), and passes the merged DataFrame to `compute_zone_comfort` (with a temporary `ComfortConfig` that maps temp/setpoint) before returning `ComfortZonePairResponse`.
- `GET /debug/flow_tracking`: looks up a zone pair, queries flow/flow_sp histories, calls `compute_flow_tracking` with `FlowTrackingConfig` (30s tolerance), and returns samples/within-band/mean-error via `FlowTrackingResponse`.
- `GET /summary/zone_index`: uses `zone_pairs_as_dicts()` to enumerate each equipment/zone root, marks which roles are wired, and returns `ZoneIndexEntry` list for a station.
- `GET /summary/zone_health`: selects one zone pair by canonical `zone`, queries the last `hours` via `compute_zone_health(...)`, wraps the result with `ZoneHealthMetricsModel`, and returns metrics + status/reasons.
- `GET /summary/building_health`: computes `compute_zone_health` for every zone in a station, sorts by status/overall score, and returns the list of `ZoneHealthMetricsModel`.
- `GET /summary/rtu_health`: fetches fan/compressor/discharge IDs from the zone pair, calls `compute_rtu_health`, converts via `rtu_health_to_dict`, and adds station/zone/equipment/hours before returning the dictionary.
- `GET /haystack/test/zoneTemps`: calls `_haystack_client.find_zone_temp_points(...)`, normalizes Haystack types with `_normalize_for_json`, and returns `{"count", "points"}`.
- `GET /haystack/test/history`: calls `_haystack_client.his_read(id, range)`, normalizes tuples to `{"ts", "val"}`, and returns the requested samples.

## 7. Analytics Functions
- `compute_zone_comfort(df: pd.DataFrame, comfort_cfg: ComfortConfig) -> dict`: expects a DataFrame with columns matching `comfort_cfg.timestamp_column`, `comfort_cfg.temp_column`, and `comfort_cfg.setpoint_column`; filters rows whose timestamp falls within the occupied window, computes `error = temp - setpoint`, and returns `{"samples": int, "within_band_pct": float|None, "mean_error_degF": float|None}`.
- `compute_flow_tracking(df_flow: pd.DataFrame, df_flow_sp: Optional[pd.DataFrame], cfg: FlowTrackingConfig | None) -> dict`: sorts and aligns flow vs. flow setpoint series via `pandas.merge_asof` (tolerance from `cfg.merge_tolerance_seconds`), computes absolute/percentage errors, compares against `cfg.pct_tolerance`/`cfg.abs_cfm_tolerance`, and reports sample count, `within_band_pct`, `mean_error_cfm`, and `mean_error_pct`.
- `compute_zone_health(station: str, zone_root: str, zone_info: Dict[str, Any], comfort_cfg: ComfortConfig, start: Optional[datetime], end: Optional[datetime]) -> ZoneHealthMetrics`: queries each role via `_query_series_df`, calls `_compute_comfort_metrics`, `_compute_flow_and_damper_metrics`, `_compute_reheat_waste_metrics`, builds an overall score, and uses `_derive_status_and_reasons` to assign `status`/`reasons`.
- `zone_health_to_dict(metrics: ZoneHealthMetrics) -> Dict[str, Any]`: flattens the dataclass to JSON-friendly structure, ensuring `status` and `reasons` are always set.
- `compute_rtu_health(station, zone_root, zone_info, start=None, end=None) -> RTUHealthMetrics`: collects fan/cooling/discharge histories, runs `_compute_binary_cycles` for binary signals and `_compute_discharge_metrics` for DA tracking, updates status/reasons (critical/warning/ok/no_data) based on short-cycle counts and discharge-air performance.
- `rtu_health_to_dict(m: RTUHealthMetrics) -> Dict[str, Any]`: returns nested dictionaries for fan/cooling/discharge metrics plus status/reasons, used directly by `/summary/rtu_health`.
- `zone_pairs_as_dicts(limit=5000) -> Dict[str, Dict[str, Dict[str, Any]]]`: builds the equipment/zone index by calling `sqlite_store.list_series()`, canonicalizing equipment to zone roots, and assigning roles via `infer_role`.

## 8. Assumptions and Conventions
- Timestamps are stored in SQLite as UTC ISO-8601 strings (`ts_utc`) via `_to_utc_iso`; when served through APIs or analytics helpers they are parsed back to naive UTC `datetime` objects using `pandas.to_datetime(..., utc=True)` and `.dt.tz_localize(None)`.
- MQTT history payloads must be JSON objects or arrays whose `messageType` is `"history"`, must contain `stationName`, a `point` object with `n:displayName`/`n:history`, and a non-empty `historyData` array with `timestamp`, `value`, and optional `status`. `decode_history_frame` tolerates malformed entries by skipping rows with bad timestamps/values.
- Names are normalized with `niagara_decode_name` (replacing `$20`/`$2d` and collapsing spaces) and `niagara_canonical_name` (snake_case keys used by `history_store` and zone pairing) so downstream indexes stay stable even if Niagara encodings vary.
- Zone role inference relies on `analytics/role_rules.py` reading `config/role_rules.json` (defaults mirror the old `ROLE_PATTERNS`), so adding custom regex/tag rules in that JSON file changes the `space_temp`/`flow`/`fan` assignments without touching code.
- Merge tolerances are centered around `30` seconds (`merge_tolerance_seconds` in `FlowTrackingConfig`, `MERGE_TOLERANCE_SECONDS` in `zone_health.py`, and the debug endpoints) so asof joins never pair samples farther apart than this window.
- Retention assumptions: `AppConfig.db_retention_hours` defaults to 720 (30 days) and is enforced by `_prune_old_rows`; `MqttJsonStreamConfig.retention_hours` defaults to 24 and limits `MqttHistoryClient`’s in-memory records; `history_store` keeps at most `_MAX_PER_SERIES = 1000` entries per `(station, history_id)`.
- Haystack integration requires `pyhaystack`; missing the dependency raises an import-time error in `niagara_client/haystack_client.py`.
