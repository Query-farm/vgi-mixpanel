/**
 * Raw events from the Mixpanel Raw Event Export API
 * (`GET data.mixpanel.com/api/2.0/export?from_date=…&to_date=…`).
 *
 * The export returns every event of a day range as newline-delimited JSON, one
 * `{"event": …, "properties": {…}}` object per line. This module turns it into a
 * resumable, streaming scan:
 *
 * - **Window.** `from_date` / `to_date` come from the named arguments or, when they
 *   are absent, from pushed `event_time` bounds. Mixpanel interprets the dates in UTC
 *   for projects created after 2023-01-01 and in the project time zone for older
 *   projects, so a bound widens the window by one day on each side; every predicate is
 *   then re-applied exactly. With no lower bound the scan reads the last
 *   `lookback_days` (default 7). The upper end is clamped to today (UTC).
 * - **Server-side filters.** `event_name = / IN` becomes the `event` JSON array;
 *   string equality / IN on the property-backed columns (distinct_id, insert_id,
 *   device_id, user_id, os, city, country, app_version, library …) becomes a `where`
 *   expression (`string(properties["$os"]) == "iOS"`), AND-ed with a caller-supplied
 *   `where_expr`.
 * - **Chunks and cursor.** The window is read `chunk_days` at a time (one export
 *   request each: the API allows 60 requests per hour, so larger chunks spend less
 *   of that budget). State holds the window, the current chunk's first day and the
 *   number of lines already consumed in it. One Arrow batch of up to 5,000 events goes
 *   out per call. The open response stream is kept for the next call in the same
 *   process; a different process (HTTP transport) re-requests the chunk and skips to
 *   the cursor, so long scans survive transport hops and rate-limit pauses.
 * - **Streaming.** The body is decoded and split into lines incrementally; nothing
 *   holds more than one batch of events in memory.
 */

import { randomUUID } from "node:crypto";
import {
  defineTableFunction,
  formatPushedFilters,
  int32,
  type TableDescriptor,
  utf8,
  type VgiDataType,
  type VgiFunction,
} from "@query-farm/vgi";
import { log } from "../log.ts";
import { configFromSecrets, MixpanelError, type MpConfig, mpFetch, SECRET_TYPE } from "./client.ts";
import {
  andJoin,
  type Constraint,
  constraintsOf,
  DAY_MS,
  eqStrings,
  propRef,
  timeBounds,
  whereParts,
} from "./pushdown.ts";

export const stringProp = (p: string) => `string(${propRef(p)})`;

import {
  asNum,
  batchOf,
  C,
  type Col,
  catalogTable,
  dayString,
  type FnExample,
  optStr,
  type Row,
  schemaOf,
  tagsOf,
  ymd,
} from "./table.ts";

export const BATCH_ROWS = 5_000;
/** An open stream left untouched this long is cancelled. */
const IDLE_MS = 10 * 60_000;
export const DEFAULT_LOOKBACK_DAYS = 7;
export const DEFAULT_CHUNK_DAYS = 7;

// ------------------------------------------------------------------ the window

export type ExportArgs = {
  from_date?: unknown;
  to_date?: unknown;
  lookback_days?: number;
  chunk_days?: number;
  where_expr?: string | null;
};

export interface ExportPlan {
  /** Inclusive day numbers (days since the epoch, UTC); fromDay > toDay = nothing to read. */
  fromDay: number;
  toDay: number;
  /** JSON array for the `event` parameter, or "". */
  event: string;
  where: string;
  /** "" = read; otherwise why nothing needs to be read. */
  empty: string;
}

const dayOf = (ms: number) => Math.floor(ms / DAY_MS);

/** Resolve the export window and server-side filters from args + pushed constraints. */
export function planExport(
  args: ExportArgs,
  constraints: Constraint[],
  props: Record<string, string>,
  anchorMs: number,
): ExportPlan {
  const today = dayOf(anchorMs);
  const lookback = Math.max(1, Math.trunc(Number(args.lookback_days ?? DEFAULT_LOOKBACK_DAYS)));
  const argFrom = args.from_date == null ? undefined : dayOf(Date.parse(ymd(args.from_date, "from_date")));
  const argTo = args.to_date == null ? undefined : dayOf(Date.parse(ymd(args.to_date, "to_date")));
  const { lower, upper } = timeBounds(constraints, "event_time");
  // One day of slack each side: legacy projects use project-local dates.
  let from = argFrom ?? (lower === undefined ? undefined : dayOf(lower) - 1);
  let to = argTo ?? (upper === undefined ? today : dayOf(upper) + 1);
  if (argFrom !== undefined && lower !== undefined) from = Math.max(argFrom, dayOf(lower) - 1);
  if (argTo !== undefined && upper !== undefined) to = Math.min(argTo, dayOf(upper) + 1);
  to = Math.min(to, today);
  if (from === undefined) from = to - lookback + 1;

  const names = eqStrings(constraints, "event_name");
  // string(): Mixpanel's `==` is type-strict and a property may hold a number.
  const where = andJoin(whereParts(constraints, props, stringProp), args.where_expr) ?? "";
  return {
    fromDay: from,
    toDay: to,
    event: names === undefined ? "" : JSON.stringify(names),
    where,
    empty: names !== undefined && names.length === 0 ? "event_name matches nothing" : from > to ? "empty window" : "",
  };
}

// ------------------------------------------------------------------ streaming

/** Split a byte stream into lines incrementally. */
async function* lines(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buf = "";
  for await (const chunk of body as unknown as AsyncIterable<Uint8Array>) {
    buf += decoder.decode(chunk, { stream: true });
    let nl = buf.indexOf("\n");
    while (nl >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.trim()) yield line;
      nl = buf.indexOf("\n");
    }
  }
  buf += decoder.decode();
  if (buf.trim()) yield buf;
}

interface OpenStream {
  chunkStart: number;
  consumed: number;
  it: AsyncGenerator<string>;
  timer: ReturnType<typeof setTimeout>;
}
const OPEN = new Map<string, OpenStream>();

function closeStream(token: string): void {
  const s = OPEN.get(token);
  if (!s) return;
  clearTimeout(s.timer);
  OPEN.delete(token);
  void s.it.return(undefined).catch(() => {});
}

function touch(token: string, s: OpenStream): void {
  clearTimeout(s.timer);
  s.timer = setTimeout(() => closeStream(token), IDLE_MS);
  s.timer.unref?.();
}

async function openChunk(cfg: MpConfig, plan: ExportPlan, start: number, end: number): Promise<AsyncGenerator<string>> {
  const { res } = await mpFetch(cfg, {
    family: "export",
    path: "/api/2.0/export",
    query: {
      from_date: dayString(start),
      to_date: dayString(end),
      event: plan.event || undefined,
      where: plan.where || undefined,
      time_in_ms: "true",
    },
  });
  if (!res.body) return (async function* () {})();
  return lines(res.body);
}

/** One exported line → a row (`properties` kept whole; `__name` is the event name). */
export function parseLine(line: string): Row | null {
  let j: unknown;
  try {
    j = JSON.parse(line);
  } catch {
    throw new MixpanelError(`export: Mixpanel returned a line that is not JSON: ${line.slice(0, 200)}`);
  }
  if (!j || typeof j !== "object") return null;
  const o = j as { event?: unknown; properties?: unknown; error?: unknown };
  if (typeof o.error === "string" && o.event === undefined) throw new MixpanelError(`export: ${o.error}`);
  const p = (o.properties && typeof o.properties === "object" ? o.properties : {}) as Row;
  return { __name: typeof o.event === "string" ? o.event : "", p };
}

// ------------------------------------------------------------------ columns

const P = (key: string) => (r: Row) => (r.p as Row | undefined)?.[key];

/** Event time: `time` is epoch ms with time_in_ms=true (seconds otherwise; both handled). */
const eventTime = (r: Row) => {
  const t = asNum(P("time")(r));
  return t === null ? null : t < 95_000_000_000 ? t * 1000 : t;
};

const PLATFORM: Record<string, string> = {
  web: "Web",
  iphone: "iOS",
  swift: "iOS",
  "react-native": "React Native",
  android: "Android",
  flutter: "Flutter",
  unity: "Unity",
};
const platformOf = (r: Row) => {
  const explicit = P("$platform")(r) ?? P("platform")(r);
  if (typeof explicit === "string" && explicit) return explicit;
  const lib = P("mp_lib")(r);
  if (typeof lib !== "string" || !lib) return null;
  return PLATFORM[lib.toLowerCase()] ?? lib;
};

/** Revenue: a numeric `revenue` / `$revenue` / `$amount` event property (Mixpanel has no fixed field). */
const revenueOf = (r: Row) => asNum(P("revenue")(r) ?? P("$revenue")(r) ?? P("$amount")(r));

/** Mixpanel-native event columns (`mixpanel.main.events`). */
export const EVENT_COLUMNS: Col[] = [
  C.str("event_name", "Event name, e.g. 'Sign Up'.", (r) => r.__name, false),
  C.time("event_time", "When the event happened (UTC; the `time` property).", eventTime, false),
  C.str("distinct_id", "The id Mixpanel resolves the user by (distinct_id).", P("distinct_id")),
  C.str("insert_id", "Client deduplication id ($insert_id).", P("$insert_id")),
  C.str("device_id", "Anonymous device id ($device_id).", P("$device_id")),
  C.str("user_id", "Identified user id ($user_id, simplified ID merge).", P("$user_id")),
  C.str("os", "Operating system ($os).", P("$os")),
  C.str("browser", "Browser ($browser).", P("$browser")),
  C.str("device", "Device model ($device / $model).", (r) => P("$device")(r) ?? P("$model")(r)),
  C.str("city", "City from IP geolocation ($city).", P("$city")),
  C.str("region", "Region or state ($region).", P("$region")),
  C.str("country_code", "ISO country code from IP geolocation (mp_country_code).", P("mp_country_code")),
  C.str("current_url", "Page URL for web events ($current_url).", P("$current_url")),
  C.str("app_version", "App version ($app_version_string).", P("$app_version_string")),
  C.str("lib", "SDK that sent the event (mp_lib), e.g. 'web', 'iphone', 'android', 'python'.", P("mp_lib")),
  C.str("lib_version", "SDK version ($lib_version).", P("$lib_version")),
  C.time("processing_time", "When Mixpanel ingested the event (mp_processing_time_ms).", P("mp_processing_time_ms")),
  C.json("properties", "Every event property as a JSON object, including the $-prefixed defaults.", (r) => r.p ?? {}),
];

/** The shared product-analytics vocabulary (`mixpanel.unified.events`). */
export const UNIFIED_EVENT_COLUMNS: Col[] = [
  C.lit("tool", "The analytics tool the row came from: 'mixpanel'.", "mixpanel"),
  C.str("event_id", "The tool's unique event id (Mixpanel has none: the $insert_id).", P("$insert_id")),
  C.str("event_name", "Event name.", (r) => r.__name, false),
  C.time("event_time", "When the event happened (UTC).", eventTime, false),
  C.time(
    "received_time",
    "When the tool received the event (Mixpanel mp_processing_time_ms).",
    P("mp_processing_time_ms"),
  ),
  C.str("user_id", "Your user id ($user_id; NULL for anonymous users).", P("$user_id")),
  C.str("device_id", "Device / anonymous id ($device_id).", P("$device_id")),
  C.str("distinct_id", "The id the tool resolves people by (Mixpanel distinct_id).", P("distinct_id")),
  C.str(
    "session_id",
    "Session id as text ($session_id when the SDK sets one; Mixpanel computes sessions at query time).",
    P("$session_id"),
  ),
  C.str("insert_id", "Client deduplication id ($insert_id).", P("$insert_id")),
  C.json("properties", "Event properties (JSON object).", (r) => r.p ?? {}),
  C.json("user_properties", "User properties at event time (Mixpanel export carries none: NULL).", () => null),
  C.str("platform", "Platform, e.g. 'iOS', 'Android', 'Web' ($platform, else derived from mp_lib).", platformOf),
  C.str("os", "Operating system ($os).", P("$os")),
  C.str("country", "Country code (mp_country_code).", P("mp_country_code")),
  C.str("city", "City ($city).", P("$city")),
  C.str("app_version", "App version ($app_version_string).", P("$app_version_string")),
  C.str("library", "SDK that sent the event (mp_lib).", P("mp_lib")),
  C.num("revenue", "Revenue attached to the event (a numeric revenue / $revenue / $amount property).", revenueOf),
];

/** Columns of each shape that push down into the export `where` expression. */
export const EVENT_PROPS: Record<string, string> = {
  distinct_id: "distinct_id",
  insert_id: "$insert_id",
  device_id: "$device_id",
  user_id: "$user_id",
  os: "$os",
  browser: "$browser",
  city: "$city",
  region: "$region",
  country_code: "mp_country_code",
  app_version: "$app_version_string",
  lib: "mp_lib",
};
export const UNIFIED_EVENT_PROPS: Record<string, string> = {
  event_id: "$insert_id",
  insert_id: "$insert_id",
  distinct_id: "distinct_id",
  device_id: "$device_id",
  user_id: "$user_id",
  os: "$os",
  city: "$city",
  country: "mp_country_code",
  app_version: "$app_version_string",
  library: "mp_lib",
};

// ------------------------------------------------------------------ the scan

interface ExportState extends ExportCursor {
  init: boolean;
  anchorMs: number;
}

/** The resumable part of an export scan's state. */
export interface ExportCursor {
  fromDay: number;
  toDay: number;
  chunk: number;
  consumed: number;
  event: string;
  where: string;
  token: string;
}

/**
 * Read the next batch of up to `batchRows` events, advancing the cursor; null when the
 * window is exhausted. Never returns an empty batch.
 */
export async function exportStep(
  cfg: MpConfig,
  state: ExportCursor,
  chunkDays: number,
  batchRows = BATCH_ROWS,
): Promise<Row[] | null> {
  const plan: ExportPlan = {
    fromDay: state.fromDay,
    toDay: state.toDay,
    event: state.event,
    where: state.where,
    empty: "",
  };
  for (;;) {
    if (state.chunk > state.toDay) {
      closeStream(state.token);
      return null;
    }
    const end = Math.min(state.chunk + chunkDays - 1, state.toDay);
    let s = OPEN.get(state.token);
    if (!s || s.chunkStart !== state.chunk || s.consumed !== state.consumed) {
      closeStream(state.token);
      const it = await openChunk(cfg, plan, state.chunk, end);
      // Resuming in another process: skip what earlier batches already returned.
      for (let i = 0; i < state.consumed; i++) {
        if ((await it.next()).done) break;
      }
      s = { chunkStart: state.chunk, consumed: state.consumed, it, timer: setTimeout(() => {}, 0) };
      OPEN.set(state.token, s);
    }
    touch(state.token, s);
    const rows: Row[] = [];
    let done = false;
    while (rows.length < batchRows) {
      const n = await s.it.next();
      if (n.done) {
        done = true;
        break;
      }
      s.consumed++;
      const row = parseLine(n.value);
      if (row) rows.push(row);
    }
    state.consumed = s.consumed;
    if (done) {
      closeStream(state.token);
      state.chunk = end + 1;
      state.consumed = 0;
    }
    if (rows.length) return rows;
  }
}

/** Drop this process's open export streams (a resume then re-requests and skips, as another process would). */
export function closeAllStreams(): void {
  for (const token of [...OPEN.keys()]) closeStream(token);
}

export interface ExportSpec {
  name: string;
  description: string;
  title: string;
  category: "events" | "unified";
  keywords: string;
  docLlm: string;
  docMd: string;
  pushdown: string;
  columns: Col[];
  props: Record<string, string>;
  withArgs: boolean;
  examples: FnExample[];
}

const PUSHDOWN_MD =
  "`event_time` ranges choose the export `from_date`/`to_date` (±1 day for legacy project-time-zone projects); " +
  "`event_name =`/`IN` becomes the `event` array; string `=`/`IN` on the property-backed columns becomes a " +
  "`where` expression. Everything is re-applied exactly.";

export { PUSHDOWN_MD as EXPORT_PUSHDOWN_MD };

export function defineExportFunction(spec: ExportSpec, fnName = spec.name): VgiFunction {
  const outSchema = schemaOf(spec.columns);
  const args: Record<string, VgiDataType> = spec.withArgs
    ? { from_date: utf8(), to_date: utf8(), where_expr: utf8(), lookback_days: int32, chunk_days: int32 }
    : {};
  return defineTableFunction<ExportArgs, ExportState>({
    name: fnName,
    description: spec.description,
    args,
    argDefaults: spec.withArgs
      ? {
          from_date: null,
          to_date: null,
          where_expr: null,
          lookback_days: DEFAULT_LOOKBACK_DAYS,
          chunk_days: DEFAULT_CHUNK_DAYS,
        }
      : {},
    argDocs: spec.withArgs
      ? {
          from_date:
            "First day to export, 'YYYY-MM-DD' (inclusive). Default: from an event_time filter, else today - lookback_days + 1.",
          to_date:
            "Last day to export, 'YYYY-MM-DD' (inclusive). Default: from an event_time filter, else today (UTC).",
          where_expr:
            'A Mixpanel segmentation expression sent as the export `where`, e.g. \'properties["plan"] == "pro"\'.',
          lookback_days: "Days read when there is no lower bound on event_time or from_date (default 7).",
          chunk_days: "Days per export request (default 7). The export API allows 60 requests per hour.",
        }
      : {},
    argConstraints: spec.withArgs
      ? {
          from_date: { pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
          to_date: { pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
          lookback_days: { ge: 1, le: 3650 },
          chunk_days: { ge: 1, le: 366 },
        }
      : undefined,
    filterPushdown: true,
    autoApplyFilters: true,
    onBind: (params) =>
      params.resolvedSecretsProvided
        ? { outputSchema: outSchema }
        : { outputSchema: outSchema, lookupSecretTypes: [SECRET_TYPE] },
    initialState: () => ({
      init: false,
      fromDay: 0,
      toDay: -1,
      chunk: 0,
      consumed: 0,
      event: "",
      where: "",
      anchorMs: Date.now(),
      token: randomUUID(),
    }),
    process: async (params, state, out) => {
      const cfg = configFromSecrets(params.secrets);
      const chunkDays = Math.max(1, Number(params.args.chunk_days ?? DEFAULT_CHUNK_DAYS));
      if (!state.init) {
        const constraints = constraintsOf(params.pushdownFilters);
        log.debug(`${fnName}: pushed filters ${formatPushedFilters(params.pushdownFilters)}`);
        const plan = planExport(
          { ...params.args, where_expr: optStr(params.args.where_expr) },
          constraints,
          spec.props,
          state.anchorMs,
        );
        state.init = true;
        state.fromDay = plan.fromDay;
        state.toDay = plan.empty ? plan.fromDay - 1 : plan.toDay;
        state.chunk = plan.fromDay;
        state.event = plan.event;
        state.where = plan.where;
        log.debug(`${fnName}: window ${dayString(plan.fromDay)}..${dayString(plan.toDay)} ${plan.empty}`);
      }
      const rows = await exportStep(cfg, state, chunkDays);
      if (rows === null) out.finish();
      else out.emit(batchOf(spec.columns, rows, params.outputSchema));
    },
    tags: tagsOf(spec),
    examples: spec.examples,
  });
}

/** The unified events table: the export scan without arguments. */
export function exportCatalogTable(spec: ExportSpec, schemaPath: string[]): TableDescriptor {
  const fn = defineExportFunction(spec, `${schemaPath.join("_")}_${spec.name}_scan`);
  return catalogTable(spec, fn, schemaPath, []);
}
