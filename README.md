<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A Query.Farm VGI worker for DuckDB.</em></p>

# Query Mixpanel Events, Funnels, Retention and User Profiles in DuckDB

> **vgi-mixpanel** · a [Query.Farm](https://query.farm) VGI worker · written in TypeScript

[![CI](https://github.com/Query-farm/vgi-mixpanel/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-mixpanel/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Mixpanel product analytics as SQL, read-only: raw events streamed from the
[Raw Event Export API](https://docs.mixpanel.com/reference/raw-event-export), segmentation,
funnel and retention reports from the [Query API](https://docs.mixpanel.com/reference/query-api),
user profiles and cohorts, the Lexicon data dictionary and annotations. A `unified` schema
repeats events, users, cohorts and definitions in the column names shared with the Amplitude
and PostHog workers.

```sql
CREATE SECRET mixpanel (TYPE mixpanel, USERNAME 'svc.reporting', SECRET '…', PROJECT_ID '123456', REGION 'eu');

-- Sign-ups per OS over three days: dates, event name and $os are all filtered by Mixpanel.
SELECT os, count(*) AS signups
FROM mixpanel.main.events()
WHERE event_name = 'Sign Up' AND os IN ('iOS', 'Android')
  AND event_time >= TIMESTAMPTZ '2026-11-01' AND event_time < TIMESTAMPTZ '2026-11-04'
GROUP BY os;

-- Day-1 retention of each day's sign-ups.
SELECT cohort_date, retention_rate
FROM mixpanel.main.retention('2026-11-01', '2026-11-07', born_event := 'Sign Up')
WHERE period = 1 ORDER BY cohort_date;

-- The same query across tools.
SELECT tool, event_name, count(*) FROM mixpanel.unified.events
WHERE event_time >= now() - INTERVAL 1 DAY GROUP BY ALL
UNION ALL
SELECT tool, event_name, count(*) FROM amplitude.unified.events
WHERE event_time >= now() - INTERVAL 1 DAY GROUP BY ALL;
```

## Functions and tables

`mixpanel.main` (Mixpanel-native):

| Object | Kind | What it returns | Mixpanel API |
|---|---|---|---|
| `events(from_date :=, to_date :=, where_expr :=, lookback_days := 7, chunk_days := 7)` | table function | one row per raw event: `event_name`, `event_time`, `distinct_id`, `insert_id`, `device_id`, `user_id`, `os`, `browser`, `device`, `city`, `region`, `country_code`, `current_url`, `app_version`, `lib`, `lib_version`, `processing_time`, `properties` JSON | `GET /api/2.0/export` |
| `segmentation(event, from_date, to_date, segment_by :=, where_expr :=, unit := 'day', count_type := 'general', segment_limit := 60)` | table function | long rows: `event`, `segment`, `period`, `period_start`, `value` | `/api/query/segmentation` |
| `funnels` | table | saved funnels: `funnel_id`, `name` | `/api/query/funnels/list` |
| `funnel(funnel_id, from_date, to_date, unit :=, funnel_window :=, window_unit :=, where_expr :=)` | table function | one row per period and step: counts, step/overall conversion, average times | `/api/query/funnels` |
| `retention(from_date, to_date, born_event :=, return_event :=, retention_type := 'birth', unit := 'day', interval_count := 7, born_where :=, where_expr :=)` | table function | one row per cohort date and period: `cohort_size`, `retained`, `retention_rate` | `/api/query/retention` |
| `top_events(count_type :=, max_events :=)` | table function | today's top events with `amount` and `percent_change` | `/api/query/events/top` |
| `event_names(count_type :=, max_events :=)` | table function | the most common event names of the last 31 days | `/api/query/events/names` |
| `top_event_properties(event, max_properties :=)` | table function | an event's most common properties with counts | `/api/query/events/properties/top` |
| `event_property_values(event, property_name, max_values :=)` | table function | a property's most common values | `/api/query/events/properties/values` |
| `profiles(where_expr :=, cohort_id :=)` | table function | user profiles: `distinct_id`, `email`, `name`, geo, `created_at`, `last_seen_at`, `properties` JSON | `POST /api/query/engage` |
| `cohorts` | table | saved cohorts and their sizes | `POST /api/query/cohorts/list` |
| `schemas` | table | Lexicon event and profile schemas (display name, description, hidden, dropped, tags, owners) | `/api/app/projects/{id}/schemas` |
| `schema_properties` | table | every Lexicon property: type, description, required, enum values | `/api/app/projects/{id}/schemas` |
| `annotations` | table | chart annotations with author and tags | `/api/app/projects/{id}/annotations` |

Dates are `'YYYY-MM-DD'` strings. Expressions (`where_expr`, `segment_by`, `born_where`) use
Mixpanel's [segmentation expression](https://docs.mixpanel.com/reference/segmentation-expressions)
syntax, e.g. `'properties["plan"] == "pro"'`. Read `$`-prefixed JSON keys with
`properties->>'$."$browser_version"'`.

**Not exposed:** JQL (deprecated by Mixpanel), segmented (`on`) funnels and retention (their
response shape is not documented), the Insights saved-report API, and anything that writes.

### Shared product-analytics vocabulary (`mixpanel.unified`)

The same tables, column names, types and order as `amplitude.unified` (see the
[vgi-amplitude README](https://github.com/Query-farm/vgi-amplitude#shared-product-analytics-vocabulary)).
Columns Mixpanel cannot fill are `NULL`.

| Table | Mixpanel source and mapping |
|---|---|
| `events` | raw export. `uuid` = `$insert_id` (Mixpanel has no event uuid; when absent, `mp:<distinct_id>:<time ms>:<event>`), `received_time` = `mp_processing_time_ms`, `user_id` = `$user_id`, `device_id` = `$device_id`, `distinct_id`, `session_id` = `$session_id` (only when an SDK sets one), `insert_id` = `$insert_id`, `user_properties` = NULL (the export carries none), `platform` = `$platform` else derived from `mp_lib`, `os` = `$os`, `country` = `mp_country_code`, `city` = `$city`, `app_version` = `$app_version_string`, `library` = `mp_lib`, `revenue` = numeric `revenue` / `$revenue` / `$amount` property |
| `users` | a table (Engage can enumerate profiles). `user_id` = `distinct_id` = the profile's `$distinct_id`, `device_ids` = NULL, `first_seen` = `$created`, `last_seen` = `$last_seen`, `platform` = NULL, `os`, `country` = `$country_code`, `city`, `app_version` |
| `cohorts` | `id` (text), `name`, `description`, `size` = `count`, `created_at`; `updated_at`, `is_archived`, `definition` = NULL |
| `event_definitions` | Lexicon event schemas; `is_active` = not dropped; `category` and `volume` = NULL |
| `property_definitions` | Lexicon properties; `scope` is `'event'` or `'user'` (Mixpanel has no group scope in the schema API) |

## What is pushed down

| Object | Sent to Mixpanel |
|---|---|
| `events`, `unified.events` | `event_time` bounds → `from_date` / `to_date`, widened a day each side (projects created before 2023 use project-time-zone dates) and clamped to today; `event_name =` / `IN` → the `event` array; string `=` / `IN` on `distinct_id`, `insert_id`, `device_id`, `user_id`, `os`, `browser`, `city`, `region`, `country_code` / `country`, `app_version`, `lib` / `library` → a `where` expression (`string(properties["$os"]) == "iOS"`, values JSON-escaped, IN lists as a parenthesised `or`), AND-ed with `where_expr` |
| `profiles`, `unified.users` | `distinct_id` (and `user_id`) `=` / `IN` → `distinct_id` / `distinct_ids`; string `=` / `IN` on `email`, `first_name`, `last_name`, `city`, `region`, `country_code`, `timezone`, `os` (`country`, `city`, `os` in `unified.users`) → Engage `where` |
| `schemas`, `schema_properties`, `unified.property_definitions` | `entity_type =` (`scope =`) → `/schemas/{entityType}` |
| `annotations` | `annotation_time` bounds → `fromDate` / `toDate` |
| report functions | their arguments are Mixpanel's parameters |

Every pushed predicate is re-applied exactly to the returned rows, so a translation only
ever narrows what crosses the network.

## Authentication

```sql
CREATE SECRET mixpanel (
    TYPE mixpanel,
    USERNAME 'svc.reporting',        -- service account username
    SECRET '…',                      -- service account secret (redacted)
    PROJECT_ID '123456',             -- numeric project id
    REGION 'us'                      -- data residency: us (default), eu or in
);
```

A legacy project `API_SECRET` works instead of `USERNAME` + `SECRET`. Optional fields:
`WORKSPACE_ID` (data views) and `BASE_URL`, which replaces every Mixpanel host with one origin
(a proxy, or the local fixture server). Credentials are read from the secret on each call and
never stored in scan state.

## Streaming, cursors and rate limits

- **Export streaming.** The NDJSON export is decoded and split into lines as it arrives; at
  most one batch (5,000 events) is held in memory. The window is read `chunk_days` at a time.
  The cursor (window, chunk, lines consumed) lives in the scan state, so a scan served by
  another process (HTTP transport) re-requests its chunk and skips to where it left off.
- **Engage paging.** The `session_id` from page 0 and the next page number live in the scan
  state; paging stops at a short page.
- **Rate limits.** Mixpanel allows 60 Query API calls per hour with 5 concurrent, and 60 export
  calls per hour at 3 per second. The worker keeps at most 5 Query API calls in flight per
  process, spaces export calls 350 ms apart, and retries 429 and 5xx with backoff that honours
  `Retry-After` (at most 60 s per wait, 5 attempts). A 429 that persists fails with a message
  naming the limit.

## Install

**Prerequisites:** [Bun](https://bun.sh) 1.3+ (for a source checkout).

```bash
git clone --branch <tag> https://github.com/Query-farm/vgi-mixpanel.git && cd vgi-mixpanel
bun install --frozen-lockfile
```

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
ATTACH 'mixpanel' (TYPE vgi, LOCATION '/abs/path/to/vgi-mixpanel/bin/vgi-mixpanel-worker');
```

**Haybarn 2.0** (`uvx haybarn-cli`):

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'mixpanel' (TYPE vgi, LOCATION '/abs/path/to/vgi-mixpanel/bin/vgi-mixpanel-worker');
```

`LOCATION` is the command DuckDB runs, so use an absolute path. Tagged releases ship signed
standalone executables (no Bun needed); point `LOCATION` at the downloaded binary. The same
binary serves HTTP with `--http --port 8000` (see the `Dockerfile`).

## Testing

```bash
make test-unit                 # bun unit tests (expressions, export window/resume, report shapes, auth)
make test-sql                  # test/sql/mixpanel.test through Haybarn 2.0 (stdio)
make test-sql TRANSPORT=http   # …over HTTP (also: launch)
make check                     # everything CI runs
```

No test touches the network or a real Mixpanel project. `test/mock_api/server.py` is a
stdlib-only, strict replay server over a hand-built project (`test/fixtures/mixpanel-project.json`,
shaped after Mixpanel's documented responses). It enforces Basic auth and `project_id`,
rejects unknown parameters, bad dates and future `to_date`, parses and evaluates `where` /
`on` expressions (type-strict `==`, `string()`/`defined()`, `and`/`or`/`not`), streams the
export as chunked NDJSON, pages Engage with `session_id`, computes segmentation and top
properties from the fixture events, and answers a 429 and a 503 once each so the retry paths
run. The SQL tests read its `/__requests` log to check exactly what was pushed down.

The metadata check runs with `execute: false` in CI: every object needs a `mixpanel` secret
for a live project.

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm
