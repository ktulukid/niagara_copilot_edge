# Niagara Copilot Edge Snapshot (2025-11-30)

## 1. Project Summary
- FastAPI app src/api/server.py loads AppConfig, boots the SQLite cache, starts the MQTT ingestion client, and optionally a Haystack reader before exposing health, debug, summary, and Haystack-test routes (see src/api/server.py#L24-L327).
- The primary ingestion path is the MQTT JSON history stream: 
iagara_client/mqtt_history_ingest.py converts stationName/point metadata to HistorySample objects and pushes them to both history_store and sqlite_store as soon as a frame arrives (src/niagara_client/mqtt_history_ingest.py#L18-L337).
- Niagra-facing helper clients support alternative sources (CSV export, HTTP oBIX, Niagara servlet, Haystack via pyhaystack, Analytics API, and the MQTT history snapshot client) for pulling historical slices when needed (src/niagara_client/*.py).
- Analytics modules in src/analytics/comfort.py, low.py, zone_health.py, and zone_pairs.py compute comfort scores, flow tracking/damper diagnostics, zone health summaries, and canonical zone pairing metadata that feed the summary endpoints (src/analytics/*.py).
- Configuration in src/config.py binds the site name, nested data source definitions, comfort window/columns, MQTT broker info, SQLite path/retention, and optional Haystack defaults so the service can be seeded from config/config.yaml (src/config.py#L1-L139).

## 2. Directory and Module Map
- src/
  - pi/
    - server.py: FastAPI entry point; it loads AppConfig, initializes sqlite_store, instantiates the MQTT history client + optional Haystack client, defines Pydantic response models, and wires health/debug/summary/haystack test routes (src/api/server.py#L24-L627).
  - nalytics/
    - comfort.py: compute_zone_comfort(df, comfort_cfg) calculates occupied-window setpoint error, returning sample count, percent in-band, and average offset for a zone (src/analytics/comfort.py#L1-L59).
    - low.py: FlowTrackingConfig plus compute_flow_tracking(df_flow, df_flow_sp, cfg) perform merge-as-of alignment with pct/absolute tolerances and report flow-tracking statistics (src/analytics/flow.py#L1-L118).
    - zone_health.py: compute_zone_health(...) queries SQLite via _query_series_df, runs comfort/flow/damper/reheat heuristics, derives an overall score + status, and exposes zone_health_to_dict (src/analytics/zone_health.py#L1-L545).
    - zone_pairs.py: Role-detection patterns, ZonePair dataclass, and zone_pairs_as_dicts() / ind_zone_pair() build canonical station→zone metadata from sqlite_store.list_series() (src/analytics/zone_pairs.py#L16-L349).
  - config.py: Pydantic models for all configs (DataSourceType, MqttConfig, NiagaraCsvExportConfig, MqttJsonStreamConfig, HaystackConfig, DataSourceConfig, ComfortConfig, AppConfig) and load_config() that reads config/config.yaml with optional MQTT host/port prompts (src/config.py#L1-L141).
  - main.py: Launches uvicorn pointing at src.api.server:app for local development (src/main.py#L1-L14).
  - 
iagara_client/
    - nalytics_api.py: AnalyticsApiClient marshals Niagara Analytics Web API calls into Pydantic models (src/niagara_client/analytics_api.py#L1-L77).
    - actory.py: HistoryClient protocol plus make_haystack_client/make_history_client helpers that eventually map AppConfig.data_source.type to a reader (src/niagara_client/factory.py#L1-L153).
    - haystack_client.py: HaystackConfig + HaystackHistoryClient wrapping pyhaystack to read entities and history tuples (src/niagara_client/haystack_client.py#L1-L160).
    - history_http_obix.py: NiagaraObixHistoryClient.fetch_history(...) hits an oBIX historyQuery, parses XML, and returns a timestamp/value DataFrame (src/niagara_client/history_http_obix.py#L1-L113).
    - mqtt_history_client.py: MqttHistoryClient subscribes to a JSON topic, caches raw records with _parsed_timestamp, and filters them for comfort analytics windows (src/niagara_client/mqtt_history_client.py#L1-L122).
    - mqtt_history_ingest.py: HistorySample dataclass, decode_history_frame(), and make_history_mqtt_client() parse MQTT history frames, canonicalize names, and fan out samples to history_store/sqlite_store (src/niagara_client/mqtt_history_ingest.py#L1-L337).
    - 
iagara_csv_export_client.py: NiagaraCsvExportClient.get_zone_history(...) downloads every CSV in the configured historyExports folder, concatenates them, and filters by equipment/time using comfort config columns (src/niagara_client/niagara_csv_export_client.py#L1-L140).
    - 
iagara_servlet_client.py: NiagaraHistoryServletClient reads JSON from a servlet, coerces columns to the comfort config, and returns a filtered DataFrame (src/niagara_client/niagara_servlet_client.py#L1-L69).
  - store/
    - history_store.py: In-memory buffer of up to 1,000 samples per (station, history_id) with dd_batch()/get_recent() for the debug endpoints (src/store/history_store.py#L1-L110).
    - sqlite_store.py: Durable history_samples table plus _series_meta, init(), dd_batch(), list_series(), and query_series() for analytics/readback (src/store/sqlite_store.py#L1-L253).

## 3. Configuration Models
- MqttConfig (src/config.py#L21-L24): host/port/history_topic define the MQTT broker and topic the ingestion client subscribes to; defaults to localhost:1883 and 
iagara/histories.
- NiagaraCsvExportConfig (src/config.py#L27-L33): host, percent-encoded ord_path (e.g., ile:^historyExports/AmsShop), username, password_env for the credential, and insecure_tls toggling TLS verification when using self-signed JACE certs.
- MqttJsonStreamConfig (src/config.py#L35-L44): MQTT parameters for the JSON stream path, including optional username, password_env, TLS toggle, client_id, keepalive, and etention_hours that the snapshot client uses to prune cached records.
- HaystackConfig (src/config.py#L51-L55): Base URI, Niagara/nHaystack username, password_env, and optional project for Haystack clients.
- DataSourceConfig (src/config.py#L62-L69): 	ype is a literal (
iagara_csv_export, mqtt_json_stream, or haystack), with optional config blocks for each ingestion path (CSV export, MQTT JSON stream, Haystack overrides).
- ComfortConfig (src/config.py#L75-L83): Strings for occupied_start/occupied_end clock window, column names for setpoint/temp/timestamp/equipment, and comfort_band_degF that defines the allowed error band.
- AppConfig (src/config.py#L89-L101): Site name, the nested DataSourceConfig, ComfortConfig, an mqtt config with defaults, db_path, db_retention_hours (default 30 days), and optional global haystack defaults; load_config() (src/config.py#L107-L141) reads config/config.yaml and optionally prompts for MQTT host/port when stdin is interactive.

## 4. Data Flow: Ingestion → Storage → Analytics → API
- **Ingestion**: mtt_history_ingest.decode_history_frame() enforces messageType == "history", extracts stationName, equipment/floor metadata, and historyData rows; each row is parsed into a timezone-aware HistorySample before history_store.add_batch() and sqlite_store.add_batch() are invoked (src/niagara_client/mqtt_history_ingest.py#L16-L336). Auxiliary clients (NiagaraCsvExportClient, NiagaraHistoryServletClient, NiagaraObixHistoryClient, HaystackHistoryClient, MqttHistoryClient) expose similar slices of history for downstream analytics when MQTT cannot be used (src/niagara_client/*.py).
- **Storage**: sqlite_store holds a history_samples table (columns id, station, history_id, 	s_utc, alue, status) and idx_history_samples_station_hist_ts for fast lookups; _series_meta retains equipment/floor/point/unit/tags per series, enabling richer indices (src/store/sqlite_store.py#L3-L253). history_store keeps the freshest 1,000 samples per series for the /debug/recent_memory endpoint (src/store/history_store.py#L3-L110).
- **Analytics**: zone_pairs_as_dicts() classifies historical points into roles (temperature, setpoint, flow, damper, etc.) using equipment tags/name patterns (src/analytics/zone_pairs.py#L16-L349). compute_zone_health() queries sqlite_store.query_series(), calls _compute_comfort_metrics() (via occupancy window and merge-as-of tolerance), _compute_flow_and_damper_metrics() (including compute_flow_tracking()), and _compute_reheat_waste_metrics(), then derives status/score (src/analytics/zone_health.py#L1-L545). compute_zone_comfort() and compute_flow_tracking() are reused by debug endpoints and the analytics pipeline (src/analytics/comfort.py#L1-L59, src/analytics/flow.py#L1-L118).
- **API**: Summary endpoints call zone_pairs_as_dicts()/ind_zone_pair() and the analytics functions above, Haystack test endpoints proxy the optional HaystackHistoryClient, and debug endpoints surface history_store or manual merges to show raw samples (src/api/server.py#L267-L627).

## 5. Database Schema
| Table | Columns | Notes |
| --- | --- | --- |
| history_samples | id (INTEGER PK), station (TEXT), history_id (TEXT), 	s_utc (TEXT ISO 8601 UTC), alue (REAL), status (TEXT) | Created/dropped in _init_schema() on sqlite_store.init(), written by dd_batch() (with _to_utc_iso() conversion and _series_meta updates) and pruned by _prune_old_rows() using db_retention_hours (src/store/sqlite_store.py#L3-L253).
| Index | idx_history_samples_station_hist_ts on (station, history_id, 	s_utc) | Supports list_series() and query_series() for efficient station+history lookups (src/store/sqlite_store.py#L45-L124).
Read-side helpers: list_series(limit) emits distinct series plus cached metadata; query_series(station, history_id, start, end) returns ordered rows with 	s/alue/status for analytics consumers (src/store/sqlite_store.py#L162-L252).

## 6. API Endpoints
- GET /health (src/api/server.py#L259-L263): returns HealthResponse(status, site_name) using _config.site_name; no other services called.
- GET /debug/recent_memory (src/api/server.py#L267-L283): queries history_store.get_recent(station, history_id, limit) and returns list of HistorySampleJson objects with timestamp/value/status (src/api/server.py#L267-L283).
- GET /debug/zone_pairs (src/api/server.py#L286-L343): flattens zone_pairs_as_dicts() and filters by optional station/zone, returning rich ZonePairResponse records with equipment, role history IDs, and 
:displayName labels (src/api/server.py#L286-L343).
- GET /debug/comfort_zone_pair (src/api/server.py#L346-L408): queries two histories via sqlite_store.query_series, merges them with configurable tolerance, calls compute_zone_comfort() with a temporary ComfortConfig, and returns ComfortZonePairResponse with computed metrics (src/api/server.py#L346-L408).
- GET /debug/flow_tracking (src/api/server.py#L411-L464): validates zone via ind_zone_pair, pulls history for flow/setpoint, runs compute_flow_tracking() (with FlowTrackingConfig.merge_tolerance_seconds=30), and returns FlowTrackingResponse normalized to FlowTrackingMetricsModel (src/api/server.py#L411-L464).
- GET /summary/zone_index (src/api/server.py#L470-L509): enumerates zones via zone_pairs_as_dicts(), reports equipment/floor/having flags, and sorts by equipment before returning ZoneIndexEntry list (src/api/server.py#L470-L509).
- GET /summary/zone_health (src/api/server.py#L512-L535): resolves a single zone with ind_zone_pair(), computes history via compute_zone_health(), and replies with ZoneHealthMetricsModel(**zone_health_to_dict(metrics)) (src/api/server.py#L512-L535).
- GET /summary/building_health (src/api/server.py#L538-L575): iterates all zones for a station, calls compute_zone_health() per zone, sorts by status/score, and returns a list of ZoneHealthMetricsModel entries (src/api/server.py#L538-L575).
- GET /haystack/test/zoneTemps (src/api/server.py#L580-L599): if _haystack_client is configured, calls ind_zone_temp_points(site_ref, limit=500) and normalizes the response for JSON; otherwise returns 500 error (src/api/server.py#L580-L599).
- GET /haystack/test/history (src/api/server.py#L601-L627): requires _haystack_client, calls his_read(id, range), and responds with the requested sample tuples (	s as ISO string, al as float) along with the id/range (src/api/server.py#L601-L627).

## 7. Analytics Functions
- compute_zone_comfort(df, comfort_cfg) (src/analytics/comfort.py#L12-L59): filters df by the comfort window (occupied_start/occupied_end), aligns 	imestamp_column, computes error = temp_column - setpoint_column, and returns a dict with samples, within_band_pct, and mean_error_degF; used by debug/comfort_zone_pair plus compute_zone_health().
- FlowTrackingConfig / compute_flow_tracking() (src/analytics/flow.py#L11-L118): config holds timestamp/value column names, pct_tolerance, optional bs_cfm_tolerance, and merge_tolerance_seconds; the function merges flow and optionally flow setpoint frames via pd.merge_asof, computes absolute/percent errors, applies the tighter of pct/absolute limits, and returns samples, within_band_pct, mean_error_cfm, and mean_error_pct for downstream diagnostics.
- compute_zone_health(...) (src/analytics/zone_health.py#L17-L542): signature accepts station/zone_root, zone metadata, ComfortConfig, and time bounds, queries sqlite_store series, reuses comfort/flow/damper/reheat helpers, calculates an overall score, and sets status/easons before returning a ZoneHealthMetrics dataclass that zone_health_to_dict() serializes for endpoints.
- zone_pairs_as_dicts() / uild_zone_pair_index() (src/analytics/zone_pairs.py#L238-L349): fetch distinct series via sqlite_store.list_series(), group them by station/equipment, infer roles via ROLE_PATTERNS based on point_name/history_id/	ags, and emit canonical zone metadata consumed by summary and debug endpoints.

## 8. Assumptions and Conventions
- MQTT history frames must be JSON objects with messageType == "history", stationName, point describing 
:name/
:history, and historyData rows; timestamps like 2025-11-29 00:30:00.249-0700 are parsed to timezone-aware datetime objects before being converted to UTC for storage (src/niagara_client/mqtt_history_ingest.py#L98-L265).
- 
iagara_decode_name()/
iagara_canonical_name() normalize Niagara escaping and non-alphanumeric characters to consistent canonical keys for history_store and indexes (src/niagara_client/mqtt_history_ingest.py#L18-L62).
- history_store caps at 1,000 samples per series, while sqlite_store retains rows for db_retention_hours (default 30 days) via _prune_old_rows() (src/store/history_store.py#L22-L110, src/store/sqlite_store.py#L93-L160).
- All sqlite_store timestamps are stored as UTC ISO strings (	s_utc) via _to_utc_iso() and deserialised into naive UTC datetimes by _query_series_df() before analytics ingest (src/store/sqlite_store.py#L80-L160, src/analytics/zone_health.py#L63-L95).
- Comfort metrics honor the string window defined by occupied_start/occupied_end and the band comfort_band_degF; anything outside that window is ignored (src/config.py#L75-L83, src/analytics/comfort.py#L12-L59).
- Flow tracking and damper heuristics use MERGE_TOLERANCE_SECONDS = 30 and the FlowTrackingConfig.pct_tolerance default of 0.1, optionally clamping to bs_cfm_tolerance, ensuring comparisons use nearest neighbors and reject sparse matches (src/analytics/flow.py#L11-L118, src/analytics/zone_health.py#L14-L268).
- Haystack/CSV clients rely on environment variables for password_env (src/niagara_client/niagara_csv_export_client.py#L26-L140, src/niagara_client/niagara_servlet_client.py#L20-L69, src/niagara_client/haystack_client.py#L7-L160) and expect TLS defaults unless insecure_tls is true.
- MqttHistoryClient retains recent JSON rows for MqttJsonStreamConfig.retention_hours (default 24h) and uses _parsed_timestamp for filtering; this in-memory client is separate from the store-focused ingestion pipeline (src/niagara_client/mqtt_history_client.py#L18-L122).
- The development entrypoint is src/main.py, which runs Uvicorn on  .0.0.0:8000 with auto-reload (src/main.py#L1-L14).
