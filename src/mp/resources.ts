/**
 * Every Mixpanel object this worker exposes, as specs for the factories in
 * `table.ts` (request-per-batch endpoints) and `export.ts` (raw events).
 *
 * `main` holds the Mixpanel-native shapes; `unified` repeats events, users,
 * cohorts and the data dictionary in the shared product-analytics columns used by
 * the Amplitude and PostHog workers so the same query UNIONs across tools.
 */

import { int32, int64, type TableDescriptor, utf8, type VgiFunction } from "@query-farm/vgi";
import { MixpanelError } from "./client.ts";
import {
  DEFAULT_CHUNK_DAYS,
  DEFAULT_LOOKBACK_DAYS,
  defineExportFunction,
  EVENT_COLUMNS,
  EVENT_PROPS,
  EXPORT_PUSHDOWN_MD,
  exportCatalogTable,
  stringProp,
  UNIFIED_EVENT_COLUMNS,
  UNIFIED_EVENT_PROPS,
} from "./export.ts";
import { andJoin, type Constraint, eqStrings, eqValues, timeBounds, whereParts } from "./pushdown.ts";
import {
  asNum,
  C,
  type Col,
  DEFAULT_ROWS,
  dayString,
  defineMpCatalogTable,
  defineMpTable,
  optInt,
  optStr,
  type Req,
  type Row,
  ymd,
} from "./table.ts";

const M = "mixpanel.main";
const U = "mixpanel.unified";
const MAIN = ["main"];
const UNIFIED = ["unified"];
const tool = C.lit("tool", "The analytics tool the row came from: 'mixpanel'.", "mixpanel");

const DATE_PATTERN = { pattern: "^\\d{4}-\\d{2}-\\d{2}$" };
const COUNT_TYPES = { choices: ["general", "unique", "average"] };

/** A required date argument → `YYYY-MM-DD`. */
function reqDate(v: unknown, name: string): string {
  if (v == null || v === "") throw new MixpanelError(`${name} is required ('YYYY-MM-DD')`);
  return ymd(v, name);
}

function checkRange(from: string, to: string): void {
  if (from > to) throw new MixpanelError(`from_date ${from} is after to_date ${to}`);
}

// ================================================================== raw events

export const events: VgiFunction = defineExportFunction({
  name: "events",
  description: "Raw Mixpanel events from the Raw Event Export API, streamed day range by day range",
  title: "Raw Events",
  category: "events",
  keywords: "mixpanel, events, raw export, event stream, clickstream, tracking, ndjson",
  docLlm:
    "Every raw event of the Mixpanel project from the Raw Event Export API, one row per event: event_name, " +
    "event_time (UTC), distinct_id, insert_id, device_id, user_id, os, browser, device, city, region, " +
    "country_code, current_url, app_version, lib, lib_version, processing_time and the full properties JSON " +
    "(read others with properties->>'$browser_version'). Choose the days with WHERE event_time >= / < ... " +
    "(pushed down) or from_date := / to_date := ('YYYY-MM-DD'); with neither it reads the last lookback_days " +
    "(7). WHERE event_name = / IN and string = / IN on distinct_id, insert_id, device_id, user_id, os, " +
    "browser, city, region, country_code, app_version and lib are filtered by Mixpanel; where_expr := sends " +
    'any Mixpanel segmentation expression (e.g. \'properties["plan"] == "pro"\'). Each chunk_days (7) of ' +
    "window is one export request; Mixpanel allows 60 per hour.",
  docMd:
    "## events\n\nRaw events from the [Raw Event Export API](https://docs.mixpanel.com/reference/raw-event-export), " +
    "streamed and resumable.\n\nArguments: `from_date`, `to_date` ('YYYY-MM-DD'), `where_expr` (a Mixpanel " +
    "expression), `lookback_days` (7), `chunk_days` (7).",
  pushdown: EXPORT_PUSHDOWN_MD,
  columns: EVENT_COLUMNS,
  props: EVENT_PROPS,
  withArgs: true,
  examples: [
    {
      sql: `SELECT os, count(*) AS signups FROM ${M}.events() WHERE event_name = 'Sign Up' AND event_time >= TIMESTAMPTZ '2026-11-01' AND event_time < TIMESTAMPTZ '2026-11-04' GROUP BY os ORDER BY signups DESC`,
      description: "Sign-ups per operating system over three days (event name and dates are sent to Mixpanel)",
    },
    {
      sql: `SELECT distinct_id, count(*) AS purchases, sum((properties->>'revenue')::DOUBLE) AS revenue FROM ${M}.events(from_date := '2026-11-01', to_date := '2026-11-03') WHERE event_name = 'Purchase' GROUP BY distinct_id ORDER BY revenue DESC`,
      description: "Revenue per user from Purchase events",
    },
  ],
});

// ================================================================== Query API reports

type SegArgs = {
  event: string;
  from_date: string;
  to_date: string;
  segment_by?: string | null;
  where_expr?: string | null;
  unit?: string;
  count_type?: string;
  segment_limit?: number;
};

/** Segmentation `data.values` → long rows (key = event or segment value, then period). */
export function segmentationRows(body: unknown, segmented: boolean, event: string): Row[] {
  const values = (body as { data?: { values?: Record<string, Record<string, unknown>> } } | null)?.data?.values ?? {};
  const rows: Row[] = [];
  for (const key of Object.keys(values).sort()) {
    const series = values[key] ?? {};
    for (const period of Object.keys(series).sort()) {
      rows.push({
        event: segmented ? event : key,
        segment: segmented ? key : null,
        period,
        value: asNum(series[period]),
      });
    }
  }
  return rows;
}

export const segmentation: VgiFunction = defineMpTable<SegArgs>({
  name: "segmentation",
  description: "Event counts per period (optionally per property value) from the Query API segmentation report",
  title: "Segmentation Report",
  category: "reports",
  keywords: "mixpanel, segmentation, insights, event counts, trends, time series, breakdown",
  docLlm:
    "Mixpanel segmentation (Insights) report as long rows: event, segment (the property value when " +
    "segment_by is set, else NULL), period (Mixpanel's bucket label), period_start (the bucket start as a " +
    "project-local TIMESTAMP) and value. Arguments: event (one event name), from_date, to_date " +
    "('YYYY-MM-DD', inclusive), segment_by := a property expression such as 'properties[\"$os\"]', " +
    "where_expr := a filter expression, unit := minute/hour/day/week/month (default day), count_type := " +
    "general (all events), unique (unique users) or average (default general), segment_limit := top N " +
    "segment values (default 60). One Query API call (60 per hour).",
  docMd:
    "## segmentation\n\nThe [segmentation query](https://docs.mixpanel.com/reference/segmentation-query): " +
    "counts of one event per period, optionally broken down by a property. Columns: `event`, `segment`, " +
    "`period`, `period_start`, `value`.",
  args: {
    event: utf8(),
    from_date: utf8(),
    to_date: utf8(),
    segment_by: utf8(),
    where_expr: utf8(),
    unit: utf8(),
    count_type: utf8(),
    segment_limit: int32,
  },
  argDefaults: { segment_by: null, where_expr: null, unit: "day", count_type: "general", segment_limit: 60 },
  argDocs: {
    event: "The event to count, e.g. 'Sign Up'.",
    from_date: "First day, 'YYYY-MM-DD' (inclusive, project time zone).",
    to_date: "Last day, 'YYYY-MM-DD' (inclusive).",
    segment_by: "Property expression to break down by, e.g. 'properties[\"$os\"]' (Mixpanel `on`).",
    where_expr: 'Mixpanel expression filtering the events, e.g. \'properties["plan"] == "pro"\'.',
    unit: "Bucket size: minute, hour, day, week or month.",
    count_type: "general (every event), unique (unique users) or average (events per user).",
    segment_limit: "Top segment values returned when segment_by is set (1-10000).",
  },
  argConstraints: {
    from_date: DATE_PATTERN,
    to_date: DATE_PATTERN,
    unit: { choices: ["minute", "hour", "day", "week", "month"] },
    count_type: COUNT_TYPES,
    segment_limit: { ge: 1, le: 10000 },
  },
  columns: [
    C.str("event", "Event name.", "event", false),
    C.str("segment", "Value of the segment_by property (NULL when not segmented).", "segment"),
    C.str("period", "Mixpanel's bucket label, e.g. '2026-11-01' or '2026-11-01 10:00:00'.", "period", false),
    C.localTime("period_start", "Bucket start in the project's time zone.", "period", false),
    C.num("value", "The count (or average) for the bucket.", "value"),
  ],
  requests: ({ args }) => {
    const from = reqDate(args.from_date, "from_date");
    const to = reqDate(args.to_date, "to_date");
    checkRange(from, to);
    if (!optStr(args.event)) throw new MixpanelError("event is required");
    const on = optStr(args.segment_by);
    return [
      {
        family: "query",
        path: "/api/query/segmentation",
        query: {
          event: args.event,
          from_date: from,
          to_date: to,
          on,
          where: optStr(args.where_expr),
          unit: args.unit ?? "day",
          type: args.count_type ?? "general",
          limit: on ? (optInt(args.segment_limit) ?? 60) : undefined,
        },
        ctx: { segmented: !!on, event: args.event },
      },
    ];
  },
  rows: (body, req) => segmentationRows(body, !!req.ctx?.segmented, String(req.ctx?.event ?? "")),
  examples: [
    {
      sql: `SELECT period_start::DATE AS day, value AS signups FROM ${M}.segmentation('Sign Up', '2026-11-01', '2026-11-03') ORDER BY day`,
      description: "Daily sign-ups over three days",
    },
    {
      sql: `SELECT segment AS os, sum(value) AS signups FROM ${M}.segmentation('Sign Up', '2026-11-01', '2026-11-03', segment_by := 'properties["$os"]') GROUP BY os ORDER BY signups DESC`,
      description: "Sign-ups per operating system",
    },
  ],
});

export const funnelsList: TableDescriptor = defineMpCatalogTable(
  {
    name: "funnels",
    description: "Saved funnels of the project (id and name), for use with funnel()",
    title: "Saved Funnels",
    category: "reports",
    keywords: "mixpanel, funnels, saved funnels, conversion",
    docLlm:
      "Every saved funnel of the project: funnel_id (pass to mixpanel.main.funnel) and name. One Query API " +
      "call (/api/query/funnels/list).",
    docMd: "## funnels\n\nSaved funnels: `funnel_id`, `name`.",
    columns: [C.big("funnel_id", "Funnel id.", "funnel_id", false), C.str("name", "Funnel name.", "name")],
    requests: () => [{ family: "query", path: "/api/query/funnels/list" }],
    examples: [{ sql: `SELECT funnel_id, name FROM ${M}.funnels ORDER BY name`, description: "Saved funnels" }],
  },
  MAIN,
  ["funnel_id"],
);

type FunnelArgs = {
  funnel_id: bigint | number;
  from_date: string;
  to_date: string;
  unit?: string | null;
  funnel_window?: number | null;
  window_unit?: string | null;
  where_expr?: string | null;
};

/** Funnel `data[date].steps` → one row per period and step. */
export function funnelRows(body: unknown): Row[] {
  const data = (body as { data?: Record<string, { steps?: Row[] }> } | null)?.data ?? {};
  const rows: Row[] = [];
  for (const period of Object.keys(data).sort()) {
    const steps = data[period]?.steps;
    if (!Array.isArray(steps)) continue;
    steps.forEach((s, i) => {
      rows.push({ ...s, period, step: i + 1, step_count: steps.length });
    });
  }
  return rows;
}

export const funnel: VgiFunction = defineMpTable<FunnelArgs>({
  name: "funnel",
  description: "A saved funnel's step counts and conversion per period from the Query API funnels report",
  title: "Funnel Report",
  category: "reports",
  keywords: "mixpanel, funnel, conversion, steps, drop-off, conversion rate",
  docLlm:
    "One saved Mixpanel funnel as long rows, one per period and step: period_start (DATE), step (1-based), " +
    "step_count, event, goal, step_label, count (users reaching the step), step_conv_ratio (from the previous " +
    "step), overall_conv_ratio (from step 1), avg_time and avg_time_from_start (seconds). Arguments: funnel_id " +
    "(from mixpanel.main.funnels), from_date, to_date ('YYYY-MM-DD'), unit := day/week/month (bucket), " +
    "funnel_window := and window_unit := (day/hour/minute/second; completion window, default the saved one), " +
    "where_expr := a filter expression. One Query API call. Segmenting a funnel (`on`) is not exposed.",
  docMd:
    "## funnel\n\nThe [funnels query](https://docs.mixpanel.com/reference/funnels-query) for one saved funnel: " +
    "a row per period and step with counts, conversion ratios and average times.",
  args: {
    funnel_id: int64(),
    from_date: utf8(),
    to_date: utf8(),
    unit: utf8(),
    funnel_window: int32,
    window_unit: utf8(),
    where_expr: utf8(),
  },
  argDefaults: { unit: null, funnel_window: null, window_unit: null, where_expr: null },
  argDocs: {
    funnel_id: "Saved funnel id (see mixpanel.main.funnels).",
    from_date: "First day, 'YYYY-MM-DD' (inclusive).",
    to_date: "Last day, 'YYYY-MM-DD' (inclusive).",
    unit: "Bucket: day, week or month (default: Mixpanel's 1-day interval).",
    funnel_window: "Time a user has to complete the funnel, in window_unit (Mixpanel `length`).",
    window_unit: "Unit of funnel_window: day, hour, minute or second.",
    where_expr: "Mixpanel expression filtering the events.",
  },
  argConstraints: {
    from_date: DATE_PATTERN,
    to_date: DATE_PATTERN,
    unit: { choices: ["day", "week", "month"] },
    funnel_window: { ge: 1 },
    window_unit: { choices: ["day", "hour", "minute", "second"] },
  },
  columns: [
    C.date("period_start", "First day of the period.", "period", false),
    C.int("step", "Step number, 1-based.", "step", false),
    C.int("step_count", "Number of steps in the funnel.", "step_count", false),
    C.str("event", "Event of the step (or $custom_event:<id>).", "event"),
    C.str("goal", "Step goal as Mixpanel names it.", "goal"),
    C.str("step_label", "Display label of a custom-event step.", "step_label"),
    C.big("count", "Users who reached this step in the period.", "count"),
    C.num("step_conv_ratio", "Conversion from the previous step (1.0 for step 1).", "step_conv_ratio"),
    C.num("overall_conv_ratio", "Conversion from the first step.", "overall_conv_ratio"),
    C.num("avg_time", "Average seconds from the previous step.", "avg_time"),
    C.num("avg_time_from_start", "Average seconds from the first step.", "avg_time_from_start"),
  ],
  requests: ({ args }) => {
    const from = reqDate(args.from_date, "from_date");
    const to = reqDate(args.to_date, "to_date");
    checkRange(from, to);
    const id = optInt(args.funnel_id);
    if (id === undefined) throw new MixpanelError("funnel_id is required (see mixpanel.main.funnels)");
    return [
      {
        family: "query",
        path: "/api/query/funnels",
        query: {
          funnel_id: id,
          from_date: from,
          to_date: to,
          unit: optStr(args.unit),
          length: optInt(args.funnel_window),
          length_unit: optStr(args.window_unit),
          where: optStr(args.where_expr),
        },
      },
    ];
  },
  rows: (body) => funnelRows(body),
  examples: [
    {
      sql: `SELECT step, event, count, round(overall_conv_ratio, 3) AS conversion FROM ${M}.funnel(7509, '2026-11-02', '2026-11-08', unit := 'week') ORDER BY step`,
      description: "Weekly conversion of each step of funnel 7509",
    },
  ],
});

type RetArgs = {
  from_date: string;
  to_date: string;
  born_event?: string | null;
  return_event?: string | null;
  retention_type?: string;
  unit?: string;
  interval_count?: number;
  born_where?: string | null;
  where_expr?: string | null;
};

/** Retention `{date: {counts, first}}` → one row per cohort date and period. */
export function retentionRows(body: unknown): Row[] {
  const data = (body ?? {}) as Record<string, { counts?: unknown[]; first?: unknown }>;
  const rows: Row[] = [];
  for (const date of Object.keys(data).sort()) {
    const c = data[date];
    if (!c || !Array.isArray(c.counts)) continue;
    const first = asNum(c.first);
    c.counts.forEach((n, i) => {
      const retained = asNum(n);
      rows.push({
        date,
        first,
        period: i,
        retained,
        rate: first && retained !== null ? retained / first : null,
      });
    });
  }
  return rows;
}

export const retention: VgiFunction = defineMpTable<RetArgs>({
  name: "retention",
  description: "Cohort retention (users per cohort date and period) from the Query API retention report",
  title: "Retention Report",
  category: "reports",
  keywords: "mixpanel, retention, cohort analysis, churn, returning users, stickiness",
  docLlm:
    "Mixpanel retention report as long rows: cohort_date (DATE), cohort_size (users in the cohort), period " +
    "(0, 1, 2 … units after the cohort date), retained (users who came back in that period) and " +
    "retention_rate (retained / cohort_size). Arguments: from_date, to_date ('YYYY-MM-DD'), born_event := the " +
    "event that puts a user in a cohort (required for birth retention), return_event := the event that counts " +
    "as coming back (default any event), retention_type := birth (first time) or compounded, unit := " +
    "day/week/month, interval_count := periods returned (default 7), born_where := and where_expr := filter " +
    "expressions. One Query API call. Segmenting retention (`on`) is not exposed.",
  docMd:
    "## retention\n\nThe [retention query](https://docs.mixpanel.com/reference/retention-query): one row per " +
    "cohort date and period with `cohort_size`, `retained` and `retention_rate`.",
  args: {
    from_date: utf8(),
    to_date: utf8(),
    born_event: utf8(),
    return_event: utf8(),
    retention_type: utf8(),
    unit: utf8(),
    interval_count: int32,
    born_where: utf8(),
    where_expr: utf8(),
  },
  argDefaults: {
    born_event: null,
    return_event: null,
    retention_type: "birth",
    unit: "day",
    interval_count: 7,
    born_where: null,
    where_expr: null,
  },
  argDocs: {
    from_date: "First cohort day, 'YYYY-MM-DD' (inclusive).",
    to_date: "Last cohort day, 'YYYY-MM-DD' (inclusive).",
    born_event: "Event that starts a cohort (required when retention_type is birth), e.g. 'Sign Up'.",
    return_event: "Event that counts as returning (default: any event).",
    retention_type: "birth (first occurrence of born_event) or compounded (any occurrence).",
    unit: "Period length: day, week or month.",
    interval_count: "Number of periods after period 0 (1-90).",
    born_where: "Mixpanel expression filtering the born events.",
    where_expr: "Mixpanel expression filtering the return events.",
  },
  argConstraints: {
    from_date: DATE_PATTERN,
    to_date: DATE_PATTERN,
    retention_type: { choices: ["birth", "compounded"] },
    unit: { choices: ["day", "week", "month"] },
    interval_count: { ge: 1, le: 90 },
  },
  columns: [
    C.date("cohort_date", "Day the cohort was born (start of the cohort period).", "date", false),
    C.big("cohort_size", "Users in the cohort.", "first"),
    C.int("period", "Periods after the cohort date (0 = the cohort period itself).", "period", false),
    C.big("retained", "Users from the cohort active in this period.", "retained"),
    C.num("retention_rate", "retained / cohort_size.", "rate"),
  ],
  requests: ({ args }) => {
    const from = reqDate(args.from_date, "from_date");
    const to = reqDate(args.to_date, "to_date");
    checkRange(from, to);
    const type = args.retention_type ?? "birth";
    const born = optStr(args.born_event);
    if (type === "birth" && !born) throw new MixpanelError("born_event := is required for birth retention");
    return [
      {
        family: "query",
        path: "/api/query/retention",
        query: {
          from_date: from,
          to_date: to,
          retention_type: type,
          born_event: type === "birth" ? born : undefined,
          event: optStr(args.return_event),
          born_where: optStr(args.born_where),
          where: optStr(args.where_expr),
          unit: args.unit ?? "day",
          interval_count: optInt(args.interval_count) ?? 7,
        },
      },
    ];
  },
  rows: (body) => retentionRows(body),
  examples: [
    {
      sql: `SELECT cohort_date, round(retention_rate, 3) AS day1 FROM ${M}.retention('2026-11-01', '2026-11-03', born_event := 'Sign Up') WHERE period = 1 ORDER BY cohort_date`,
      description: "Day-1 retention of each day's sign-ups",
    },
  ],
});

type TopArgs = {
  count_type?: string;
  max_events?: number;
};

export const topEvents: VgiFunction = defineMpTable<TopArgs>({
  name: "top_events",
  description: "Today's most common events with their counts and change versus yesterday",
  title: "Top Events Today",
  category: "reports",
  keywords: "mixpanel, top events, today, event volume, popular events",
  docLlm:
    "Today's top events (project time zone) from /api/query/events/top: event, amount (count so far today) " +
    "and percent_change versus the same time yesterday. count_type := general/unique/average, max_events := " +
    "how many (default 100). One Query API call.",
  docMd: "## top_events\n\nToday's most common events: `event`, `amount`, `percent_change`.",
  args: { count_type: utf8(), max_events: int32 },
  argDefaults: { count_type: "general", max_events: 100 },
  argDocs: {
    count_type: "general (all events), unique (unique users) or average.",
    max_events: "Number of events to return (1-1000).",
  },
  argConstraints: { count_type: COUNT_TYPES, max_events: { ge: 1, le: 1000 } },
  columns: [
    C.str("event", "Event name.", "event", false),
    C.big("amount", "Count so far today.", "amount"),
    C.num("percent_change", "Change versus the same time yesterday (0.1 = +10%).", "percent_change"),
  ],
  requests: ({ args }) => [
    {
      family: "query",
      path: "/api/query/events/top",
      query: { type: args.count_type ?? "general", limit: optInt(args.max_events) ?? 100 },
    },
  ],
  rows: (body) => ((body as { events?: Row[] } | null)?.events ?? []) as Row[],
  examples: [
    {
      sql: `SELECT event, amount FROM ${M}.top_events() ORDER BY amount DESC LIMIT 5`,
      description: "Five most common events today",
    },
  ],
});

export const eventNames: VgiFunction = defineMpTable<TopArgs>({
  name: "event_names",
  description: "Names of the project's most common events over the last 31 days",
  title: "Event Names",
  category: "reports",
  keywords: "mixpanel, event names, event list, tracked events",
  docLlm:
    "The names of the most common events of the last 31 days from /api/query/events/names, one row each " +
    "(rank 1 = most common). count_type := general/unique/average, max_events := how many (default 255). " +
    "For the full data dictionary use mixpanel.main.schemas.",
  docMd: "## event_names\n\nMost common event names of the last 31 days: `rank`, `event_name`.",
  args: { count_type: utf8(), max_events: int32 },
  argDefaults: { count_type: "general", max_events: 255 },
  argDocs: {
    count_type: "general (all events), unique (unique users) or average.",
    max_events: "Number of names to return (1-10000).",
  },
  argConstraints: { count_type: COUNT_TYPES, max_events: { ge: 1, le: 10000 } },
  columns: [
    C.int("rank", "Rank by volume (1 = most common).", "rank", false),
    C.str("event_name", "Event name.", "name", false),
  ],
  requests: ({ args }) => [
    {
      family: "query",
      path: "/api/query/events/names",
      query: { type: args.count_type ?? "general", limit: optInt(args.max_events) ?? 255 },
    },
  ],
  rows: (body) => (Array.isArray(body) ? body.map((name, i) => ({ name, rank: i + 1 })) : []),
  examples: [
    {
      sql: `SELECT event_name FROM ${M}.event_names() ORDER BY rank LIMIT 10`,
      description: "Ten most common event names",
    },
  ],
});

type PropTopArgs = {
  event: string;
  max_properties?: number;
};

export const topEventProperties: VgiFunction = defineMpTable<PropTopArgs>({
  name: "top_event_properties",
  description: "The most common properties of one event over the last 31 days, with counts",
  title: "Top Event Properties",
  category: "reports",
  keywords: "mixpanel, event properties, property counts, top properties",
  docLlm:
    "The most common properties sent with one event (last 31 days) from /api/query/events/properties/top: " +
    "property_name and count (events carrying it). Arguments: event, max_properties := (default 10). " +
    "Then use event_property_values(event, property_name) for its values.",
  docMd: "## top_event_properties\n\nMost common properties of an event: `property_name`, `count`.",
  args: { event: utf8(), max_properties: int32 },
  argDefaults: { max_properties: 10 },
  argDocs: { event: "Event name, e.g. 'Purchase'.", max_properties: "Number of properties (1-1000)." },
  argConstraints: { max_properties: { ge: 1, le: 1000 } },
  columns: [
    C.str("property_name", "Property name.", "name", false),
    C.big("count", "Events carrying the property.", "count"),
  ],
  requests: ({ args }) => {
    if (!optStr(args.event)) throw new MixpanelError("event is required");
    return [
      {
        family: "query",
        path: "/api/query/events/properties/top",
        query: { event: args.event, limit: optInt(args.max_properties) ?? 10 },
      },
    ];
  },
  rows: (body) =>
    Object.entries((body ?? {}) as Record<string, { count?: unknown }>)
      .map(([name, v]) => ({ name, count: v?.count ?? null }))
      .sort((a, b) => a.name.localeCompare(b.name)),
  examples: [
    {
      sql: `SELECT property_name, count FROM ${M}.top_event_properties('Purchase') ORDER BY count DESC`,
      description: "Properties of Purchase events by frequency",
    },
  ],
});

type PropValArgs = {
  event: string;
  property_name: string;
  max_values?: number;
};

export const eventPropertyValues: VgiFunction = defineMpTable<PropValArgs>({
  name: "event_property_values",
  description: "The most common values of one event property over the last 31 days",
  title: "Event Property Values",
  category: "reports",
  keywords: "mixpanel, property values, top values, distinct values",
  docLlm:
    "The most common values of one property of one event (last 31 days) from " +
    "/api/query/events/properties/values, ranked (rank 1 = most common). Arguments: event, property_name, " +
    "max_values := (default 255).",
  docMd: "## event_property_values\n\nTop values of an event property: `rank`, `value`.",
  args: { event: utf8(), property_name: utf8(), max_values: int32 },
  argDefaults: { max_values: 255 },
  argDocs: {
    event: "Event name, e.g. 'Purchase'.",
    property_name: "Property name, e.g. 'plan' or '$os'.",
    max_values: "Number of values (1-10000).",
  },
  argConstraints: { max_values: { ge: 1, le: 10000 } },
  columns: [
    C.int("rank", "Rank by frequency (1 = most common).", "rank", false),
    C.str("value", "Property value.", "value"),
  ],
  requests: ({ args }) => {
    if (!optStr(args.event) || !optStr(args.property_name)) {
      throw new MixpanelError("event and property_name are required");
    }
    return [
      {
        family: "query",
        path: "/api/query/events/properties/values",
        query: { event: args.event, name: args.property_name, limit: optInt(args.max_values) ?? 255 },
      },
    ];
  },
  rows: (body) => (Array.isArray(body) ? body.map((value, i) => ({ value, rank: i + 1 })) : []),
  examples: [
    {
      sql: `SELECT value AS plan FROM ${M}.event_property_values('Purchase', 'plan') ORDER BY rank`,
      description: "Plans seen on Purchase events",
    },
  ],
});

// ================================================================== users and cohorts

const PROFILE_PROPS: Record<string, string> = {
  email: "$email",
  first_name: "$first_name",
  last_name: "$last_name",
  city: "$city",
  region: "$region",
  country_code: "$country_code",
  timezone: "$timezone",
  os: "$os",
};

/** Engage form fields from pushed distinct_id / property constraints and the arguments. */
export function engageForm(
  constraints: Constraint[],
  props: Record<string, string>,
  idColumns: string[],
  whereExpr?: string,
  cohortId?: number,
): { form: Record<string, string | undefined>; empty: boolean } {
  let ids: string[] | undefined;
  for (const c of idColumns) {
    const vs = eqStrings(constraints, c);
    if (vs !== undefined) ids = ids ? ids.filter((x) => vs.includes(x)) : vs;
  }
  const where = andJoin(whereParts(constraints, props, stringProp), whereExpr);
  const form: Record<string, string | undefined> = { where };
  if (ids && ids.length === 1) form.distinct_id = ids[0];
  else if (ids && ids.length > 1 && ids.length <= 2000) form.distinct_ids = JSON.stringify(ids);
  if (cohortId !== undefined) form.filter_by_cohort = JSON.stringify({ id: cohortId });
  return { form, empty: ids !== undefined && ids.length === 0 };
}

const engageRows = (body: unknown): Row[] => DEFAULT_ROWS(body);
const PP = (k: string) => (r: Row) => (r.$properties as Row | undefined)?.[k];
const profileName = (r: Row) => {
  const n = PP("$name")(r);
  if (typeof n === "string" && n) return n;
  const parts = [PP("$first_name")(r), PP("$last_name")(r)].filter((x) => typeof x === "string" && x);
  return parts.length ? parts.join(" ") : null;
};

type ProfileArgs = {
  where_expr?: string | null;
  cohort_id?: bigint | number | null;
};

export const profiles: VgiFunction = defineMpTable<ProfileArgs>({
  name: "profiles",
  description: "User profiles from the Engage API, paged by session id",
  title: "User Profiles",
  category: "users",
  keywords: "mixpanel, users, user profiles, people, engage, crm, customers",
  docLlm:
    "Mixpanel user profiles (People) from the Engage API: distinct_id, email, name, first_name, last_name, " +
    "phone, city, region, country_code, timezone, os, created_at, last_seen_at and every profile property as " +
    "JSON (properties->>'plan'). WHERE distinct_id = / IN is sent as distinct_id(s); string = / IN on email, " +
    "first_name, last_name, city, region, country_code, timezone and os becomes an Engage `where` " +
    'expression. where_expr := sends any expression (e.g. \'properties["plan"] == "pro"\'); cohort_id := ' +
    "lists a cohort's members (mixpanel.main.cohorts.id; members without a profile appear with only " +
    "distinct_id). Pages of up to 1000 profiles, one Query API call each.",
  docMd:
    "## profiles\n\nUser profiles from the [Engage API](https://docs.mixpanel.com/reference/engage-query). " +
    "Arguments: `where_expr`, `cohort_id`.",
  pushdown:
    "`distinct_id` = / IN → `distinct_id` / `distinct_ids`; string = / IN on email, first_name, last_name, city, " +
    "region, country_code, timezone, os → Engage `where`.",
  args: { where_expr: utf8(), cohort_id: int64() },
  argDefaults: { where_expr: null, cohort_id: null },
  argDocs: {
    where_expr: 'Mixpanel expression over profile properties, e.g. \'properties["plan"] == "pro"\'.',
    cohort_id: "Only members of this cohort (see mixpanel.main.cohorts).",
  },
  argConstraints: { cohort_id: { ge: 1 } },
  columns: [
    C.str("distinct_id", "The profile's distinct_id.", "$distinct_id", false),
    C.str("email", "$email.", PP("$email")),
    C.str("name", "$name, else first and last name.", profileName),
    C.str("first_name", "$first_name.", PP("$first_name")),
    C.str("last_name", "$last_name.", PP("$last_name")),
    C.str("phone", "$phone.", PP("$phone")),
    C.str("city", "$city.", PP("$city")),
    C.str("region", "$region.", PP("$region")),
    C.str("country_code", "$country_code.", PP("$country_code")),
    C.str("timezone", "$timezone.", PP("$timezone")),
    C.str("os", "$os (last seen).", PP("$os")),
    C.time("created_at", "$created (read as UTC).", PP("$created")),
    C.time("last_seen_at", "$last_seen (read as UTC).", PP("$last_seen")),
    C.json("properties", "Every profile property as a JSON object.", (r) => r.$properties ?? {}),
  ],
  requests: ({ args, constraints }) => {
    const { form, empty } = engageForm(
      constraints,
      PROFILE_PROPS,
      ["distinct_id"],
      optStr(args.where_expr),
      optInt(args.cohort_id),
    );
    return empty ? [] : [{ family: "query", method: "POST", path: "/api/query/engage", form, engage: true }];
  },
  rows: engageRows,
  examples: [
    {
      sql: `SELECT email, created_at FROM ${M}.profiles(where_expr := 'properties["plan"] == "pro"') ORDER BY created_at`,
      description: "Users on the pro plan (filtered by Mixpanel)",
    },
    {
      sql: `SELECT country_code, count(*) AS users FROM ${M}.profiles() GROUP BY country_code ORDER BY users DESC`,
      description: "Users per country",
    },
  ],
});

const COHORT_REQ: Req[] = [{ family: "query", method: "POST", path: "/api/query/cohorts/list" }];

export const cohorts: TableDescriptor = defineMpCatalogTable(
  {
    name: "cohorts",
    description: "Saved cohorts with their current size",
    title: "Cohorts",
    category: "users",
    keywords: "mixpanel, cohorts, segments, audiences, user groups",
    docLlm:
      "Every saved cohort of the project from /api/query/cohorts/list: id (pass to profiles(cohort_id := )), " +
      "name, description, count (current members), is_visible, created_at, project_id.",
    docMd: "## cohorts\n\nSaved cohorts and their sizes.",
    columns: [
      C.big("id", "Cohort id.", "id", false),
      C.str("name", "Cohort name.", "name"),
      C.str("description", "Description.", "description"),
      C.big("count", "Current number of users in the cohort.", "count"),
      C.bool("is_visible", "Shown in the Mixpanel UI.", "is_visible"),
      C.time("created_at", "When the cohort was created (read as UTC).", "created"),
      C.big("project_id", "Project id.", "project_id"),
    ],
    requests: () => COHORT_REQ,
    examples: [{ sql: `SELECT name, count FROM ${M}.cohorts ORDER BY count DESC`, description: "Cohorts by size" }],
  },
  MAIN,
  ["id"],
);

// ================================================================== Lexicon and annotations

const ENTITY: Record<string, string> = { event: "event", profile: "profile" };

/** Lexicon requests: one entity type when pushed, else all schemas. */
function schemaRequests(entityTypes: string[] | undefined): Req[] {
  if (entityTypes === undefined) return [{ family: "app", path: "/api/app/projects/{project}/schemas" }];
  return entityTypes
    .filter((t) => ENTITY[t])
    .map((t) => ({ family: "app" as const, path: `/api/app/projects/{project}/schemas/${t}` }));
}

const MPMETA = (r: Row) => (r.schemaJson?.metadata?.["com.mixpanel"] ?? {}) as Row;

export const schemas: TableDescriptor = defineMpCatalogTable(
  {
    name: "schemas",
    description: "Lexicon data dictionary: one row per event or user-profile schema",
    title: "Lexicon Schemas",
    category: "lexicon",
    keywords: "mixpanel, lexicon, data dictionary, tracking plan, schemas, event definitions",
    docLlm:
      "The Lexicon (data dictionary) from /api/app/projects/{id}/schemas: entity_type ('event' or 'profile'), " +
      "name, display_name, description, is_hidden, is_dropped (data dropped at ingestion), tags, contacts, " +
      "team_contacts, property_count and the whole schema_json. WHERE entity_type = 'event' reads only event " +
      "schemas. Properties are in mixpanel.main.schema_properties.",
    docMd: "## schemas\n\nLexicon schemas for events and user profiles.",
    pushdown: "`entity_type` = / IN reads only that entity type.",
    columns: [
      C.str("entity_type", "'event' or 'profile'.", "entityType", false),
      C.str("name", "Event name (or '$user' style profile entity name).", "name", false),
      C.str("display_name", "Display name.", (r) => MPMETA(r).displayName),
      C.str("description", "Description.", "schemaJson.description"),
      C.bool("is_hidden", "Hidden in the Mixpanel UI.", (r) => MPMETA(r).hidden ?? false),
      C.bool("is_dropped", "Data is dropped at ingestion (events only).", (r) => MPMETA(r).dropped ?? false),
      C.strList("tags", "Lexicon tags.", (r) => MPMETA(r).tags),
      C.strList("contacts", "Owner emails.", (r) => MPMETA(r).contacts),
      C.strList("team_contacts", "Owning teams.", (r) => MPMETA(r).teamContacts),
      C.int(
        "property_count",
        "Properties defined in the schema.",
        (r) => Object.keys(r.schemaJson?.properties ?? {}).length,
      ),
      C.json("schema_json", "The full JSON Schema document.", "schemaJson"),
    ],
    requests: ({ constraints }) => schemaRequests(eqStrings(constraints, "entity_type")),
    examples: [
      {
        sql: `SELECT name, description FROM ${M}.schemas WHERE entity_type = 'event' AND is_hidden ORDER BY name`,
        description: "Hidden events in the Lexicon",
      },
    ],
  },
  MAIN,
  ["entity_type", "name"],
);

/** Flatten each schema's `schemaJson.properties` into property rows. */
export function propertyRows(body: unknown): Row[] {
  const out: Row[] = [];
  for (const s of DEFAULT_ROWS(body)) {
    const props = (s.schemaJson?.properties ?? {}) as Record<string, Row>;
    const required = new Set<string>(Array.isArray(s.schemaJson?.required) ? s.schemaJson.required : []);
    for (const name of Object.keys(props).sort()) {
      const p = props[name] ?? {};
      const type = Array.isArray(p.type) ? p.type.find((t: unknown) => t !== "null") : p.type;
      out.push({
        entity_type: s.entityType,
        entity_name: s.name,
        property_name: name,
        data_type: type ?? null,
        description: p.description ?? null,
        display_name: p.metadata?.["com.mixpanel"]?.displayName ?? null,
        is_hidden: p.metadata?.["com.mixpanel"]?.hidden ?? false,
        is_dropped: p.metadata?.["com.mixpanel"]?.dropped ?? false,
        is_required: required.has(name),
        enum_values: Array.isArray(p.enum) ? p.enum : null,
      });
    }
  }
  return out;
}

export const schemaProperties: TableDescriptor = defineMpCatalogTable(
  {
    name: "schema_properties",
    description: "Lexicon property definitions: one row per property of each event or profile schema",
    title: "Lexicon Properties",
    category: "lexicon",
    keywords: "mixpanel, lexicon, properties, property definitions, data dictionary",
    docLlm:
      "Every property the Lexicon defines, flattened from each schema: entity_type ('event' or 'profile'), " +
      "entity_name (the event), property_name, data_type (string, number, integer, boolean, array, object), " +
      "description, display_name, is_hidden, is_dropped, is_required, enum_values. WHERE entity_type = ... " +
      "reads one entity type.",
    docMd: "## schema_properties\n\nLexicon properties of every event and the user profile.",
    pushdown: "`entity_type` = / IN reads only that entity type.",
    columns: [
      C.str("entity_type", "'event' or 'profile'.", "entity_type", false),
      C.str("entity_name", "Event (or profile entity) the property belongs to.", "entity_name", false),
      C.str("property_name", "Property name.", "property_name", false),
      C.str("data_type", "JSON Schema type: string, number, integer, boolean, array, object.", "data_type"),
      C.str("description", "Description.", "description"),
      C.str("display_name", "Display name.", "display_name"),
      C.bool("is_hidden", "Hidden in the UI.", "is_hidden"),
      C.bool("is_dropped", "Dropped at ingestion.", "is_dropped"),
      C.bool("is_required", "Listed as required by the schema.", "is_required"),
      C.strList("enum_values", "Allowed values, when the schema has an enum.", "enum_values"),
    ],
    requests: ({ constraints }) => schemaRequests(eqStrings(constraints, "entity_type")),
    rows: (body) => propertyRows(body),
    examples: [
      {
        sql: `SELECT property_name, data_type FROM ${M}.schema_properties WHERE entity_type = 'event' AND entity_name = 'Purchase' ORDER BY property_name`,
        description: "Properties defined for the Purchase event",
      },
    ],
  },
  MAIN,
  ["entity_type", "entity_name", "property_name"],
);

export const annotations: TableDescriptor = defineMpCatalogTable(
  {
    name: "annotations",
    description: "Chart annotations (releases, campaigns, incidents) with their dates",
    title: "Annotations",
    category: "lexicon",
    keywords: "mixpanel, annotations, releases, notes, chart markers",
    docLlm:
      "Annotations from /api/app/projects/{id}/annotations: id, annotation_time (read as UTC), description, " +
      "user_id and user_name of the author, tags. WHERE annotation_time >= / <= bounds are sent as " +
      "fromDate/toDate.",
    docMd: "## annotations\n\nChart annotations.",
    pushdown: "`annotation_time` bounds → `fromDate` / `toDate` (whole days, re-applied exactly).",
    columns: [
      C.big("id", "Annotation id.", "id", false),
      C.time("annotation_time", "When the annotation is placed (read as UTC).", "date"),
      C.str("description", "Annotation text.", "description"),
      C.big("user_id", "Author's user id.", "user.id"),
      C.str(
        "user_name",
        "Author's name.",
        (r) => [r.user?.first_name, r.user?.last_name].filter((x) => typeof x === "string" && x).join(" ") || null,
      ),
      C.strList("tags", "Tag names.", (r) => (Array.isArray(r.tags) ? r.tags.map((t: Row) => t?.name) : null)),
    ],
    requests: ({ constraints }) => {
      const { lower, upper } = timeBounds(constraints, "annotation_time");
      const day = (ms: number) => dayString(Math.floor(ms / 86_400_000));
      return [
        {
          family: "app",
          path: "/api/app/projects/{project}/annotations",
          query: {
            fromDate: lower === undefined ? undefined : day(lower),
            toDate: upper === undefined ? undefined : day(upper),
          },
        },
      ];
    },
    examples: [
      {
        sql: `SELECT annotation_time, description FROM ${M}.annotations WHERE annotation_time >= TIMESTAMPTZ '2026-11-01' ORDER BY annotation_time`,
        description: "Annotations since November 1 (the date bound is sent to Mixpanel)",
      },
    ],
  },
  MAIN,
  ["id"],
);

// ================================================================== unified

export const unifiedEvents: TableDescriptor = exportCatalogTable(
  {
    name: "events",
    description: "Raw events in the shared product-analytics shape (tool, event_name, event_time, ids, properties …)",
    title: "Events (Shared Shape)",
    category: "unified",
    keywords: "product analytics, events, unified, mixpanel, amplitude, posthog, segment",
    docLlm:
      "Mixpanel raw events (Raw Event Export API) with the shared product-analytics columns: tool, event_id " +
      "($insert_id), event_name, event_time, received_time, user_id, device_id, distinct_id, session_id, " +
      "insert_id, properties, user_properties (NULL), platform, os, country, city, app_version, library, " +
      "revenue, so they UNION ALL with the same table of the Amplitude and PostHog workers. WHERE event_time " +
      `bounds choose the export days (default the last ${DEFAULT_LOOKBACK_DAYS}); event_name and string ` +
      "columns push down as for mixpanel.main.events.",
    docMd: "## unified.events\n\nMixpanel events in the shared product-analytics shape.",
    pushdown: EXPORT_PUSHDOWN_MD,
    columns: UNIFIED_EVENT_COLUMNS,
    props: UNIFIED_EVENT_PROPS,
    withArgs: false,
    examples: [
      {
        sql: `SELECT platform, count(DISTINCT distinct_id) AS users FROM ${U}.events WHERE event_time >= TIMESTAMPTZ '2026-11-01' AND event_time < TIMESTAMPTZ '2026-11-04' GROUP BY platform ORDER BY users DESC`,
        description: "Active users per platform over three days",
      },
    ],
  },
  UNIFIED,
);

const UNIFIED_USER_PROPS: Record<string, string> = { email: "$email", country: "$country_code", city: "$city" };

export const unifiedUsers: TableDescriptor = defineMpCatalogTable(
  {
    name: "users",
    description: "User profiles in the shared product-analytics shape",
    title: "Users (Shared Shape)",
    category: "unified",
    keywords: "product analytics, users, persons, profiles, unified, mixpanel",
    docLlm:
      "Mixpanel user profiles (Engage API) with the shared columns: tool, user_id and distinct_id (both the " +
      "profile distinct_id), email, name, created_at, last_seen_at, country, city, properties JSON. WHERE " +
      "user_id / distinct_id = / IN and email / country / city = / IN push down. Use mixpanel.main.profiles " +
      "for cohort filters and every field.",
    docMd: "## unified.users\n\nMixpanel user profiles in the shared shape.",
    pushdown: "`user_id` / `distinct_id` = / IN → distinct_id(s); `email`, `country`, `city` = / IN → Engage `where`.",
    columns: [
      tool,
      C.str("user_id", "Your user id (Mixpanel: the profile distinct_id).", "$distinct_id", false),
      C.str("distinct_id", "The id the tool resolves people by.", "$distinct_id", false),
      C.str("email", "Email.", PP("$email")),
      C.str("name", "Display name.", profileName),
      C.time("created_at", "When the profile was created.", PP("$created")),
      C.time("last_seen_at", "When the user was last seen.", PP("$last_seen")),
      C.str("country", "Country code.", PP("$country_code")),
      C.str("city", "City.", PP("$city")),
      C.json("properties", "Profile properties (JSON object).", (r) => r.$properties ?? {}),
    ],
    requests: ({ constraints }) => {
      const { form, empty } = engageForm(constraints, UNIFIED_USER_PROPS, ["user_id", "distinct_id"]);
      return empty ? [] : [{ family: "query", method: "POST", path: "/api/query/engage", form, engage: true }];
    },
    rows: engageRows,
    examples: [
      {
        sql: `SELECT country, count(*) AS users FROM ${U}.users GROUP BY country ORDER BY users DESC`,
        description: "Users per country",
      },
    ],
  },
  UNIFIED,
  ["distinct_id"],
);

export const unifiedCohorts: TableDescriptor = defineMpCatalogTable(
  {
    name: "cohorts",
    description: "Cohorts in the shared product-analytics shape (tool, cohort_id, name, size, timestamps, definition)",
    title: "Cohorts (Shared Shape)",
    category: "unified",
    keywords: "product analytics, cohorts, unified, mixpanel, amplitude, posthog",
    docLlm:
      "Mixpanel cohorts with the shared columns: tool, cohort_id, name, description, size, created_at, " +
      "updated_at, is_archived, definition. Mixpanel's cohort list has no update time, archive flag or " +
      "definition, so those are NULL. Use mixpanel.main.cohorts for is_visible.",
    docMd: "## unified.cohorts\n\nMixpanel cohorts in the shared shape.",
    columns: [
      tool,
      C.str("cohort_id", "Cohort id.", "id", false),
      C.str("name", "Cohort name.", "name"),
      C.str("description", "Description.", "description"),
      C.big("size", "Current members.", "count"),
      C.time("created_at", "Created.", "created"),
      C.time("updated_at", "Last modified (not reported by Mixpanel: NULL).", () => null),
      C.bool("is_archived", "Archived (not reported by Mixpanel: NULL).", () => null),
      C.json("definition", "Tool-specific definition (not reported by Mixpanel: NULL).", () => null),
    ],
    requests: () => COHORT_REQ,
    examples: [{ sql: `SELECT tool, name, size FROM ${U}.cohorts ORDER BY size DESC`, description: "Cohorts by size" }],
  },
  UNIFIED,
  ["cohort_id"],
);

export const unifiedEventDefinitions: TableDescriptor = defineMpCatalogTable(
  {
    name: "event_definitions",
    description: "Event definitions in the shared product-analytics shape (name, description, category, volume)",
    title: "Event Definitions (Shared Shape)",
    category: "unified",
    keywords: "product analytics, event definitions, tracking plan, lexicon, unified, mixpanel",
    docLlm:
      "Every Lexicon event schema with the shared columns: tool, event_name, display_name, description, " +
      "category (NULL: Mixpanel has none), is_hidden, is_active (false when the event's data is dropped), " +
      "volume (NULL: use top_events or segmentation) and tags.",
    docMd: "## unified.event_definitions\n\nLexicon events in the shared shape.",
    columns: [
      tool,
      C.str("event_name", "Event name.", "name", false),
      C.str("display_name", "Display name.", (r) => MPMETA(r).displayName),
      C.str("description", "Description.", "schemaJson.description"),
      C.str("category", "Category (Mixpanel: NULL).", () => null),
      C.bool("is_hidden", "Hidden in the tool's UI.", (r) => MPMETA(r).hidden ?? false),
      C.bool("is_active", "Receiving data (Mixpanel: not dropped).", (r) => !(MPMETA(r).dropped ?? false)),
      C.big("volume", "Recent volume (Mixpanel: NULL).", () => null),
      C.strList("tags", "Tags.", (r) => MPMETA(r).tags),
    ],
    requests: () => schemaRequests(["event"]),
    examples: [
      {
        sql: `SELECT event_name, description FROM ${U}.event_definitions WHERE NOT is_hidden ORDER BY event_name`,
        description: "Visible events of the tracking plan",
      },
    ],
  },
  UNIFIED,
  ["event_name"],
);

export const unifiedPropertyDefinitions: TableDescriptor = defineMpCatalogTable(
  {
    name: "property_definitions",
    description: "Event and user property definitions in the shared product-analytics shape",
    title: "Property Definitions (Shared Shape)",
    category: "unified",
    keywords: "product analytics, property definitions, tracking plan, lexicon, unified, mixpanel",
    docLlm:
      "Every Lexicon property with the shared columns: tool, property_name, scope ('event' or 'user'), " +
      "event_name (event scope), data_type, description, is_array, is_required, is_hidden, enum_values. WHERE " +
      "scope = 'user' reads only the profile schema.",
    docMd: "## unified.property_definitions\n\nLexicon properties in the shared shape.",
    pushdown: "`scope` = / IN reads only the event or profile schemas.",
    columns: [
      tool,
      C.str("property_name", "Property name.", "property_name", false),
      C.str("scope", "'event' or 'user'.", (r) => (r.entity_type === "profile" ? "user" : "event"), false),
      C.str("event_name", "Event the property belongs to (event scope; NULL for user properties).", (r) =>
        r.entity_type === "event" ? r.entity_name : null,
      ),
      C.str("data_type", "Data type.", "data_type"),
      C.str("description", "Description.", "description"),
      C.bool("is_array", "Array-valued.", (r) => r.data_type === "array"),
      C.bool("is_required", "Required.", "is_required"),
      C.bool("is_hidden", "Hidden.", "is_hidden"),
      C.strList("enum_values", "Allowed values.", "enum_values"),
    ],
    requests: ({ constraints }) => {
      const scopes = eqValues(constraints, "scope");
      return schemaRequests(
        scopes === undefined ? undefined : scopes.map((s) => (s === "user" ? "profile" : String(s))),
      );
    },
    rows: (body) => propertyRows(body),
    examples: [
      {
        sql: `SELECT scope, count(*) AS properties FROM ${U}.property_definitions GROUP BY scope ORDER BY scope`,
        description: "Properties per scope",
      },
    ],
  },
  UNIFIED,
  [],
);

export const MAIN_FUNCTIONS: VgiFunction[] = [
  events,
  segmentation,
  funnel,
  retention,
  topEvents,
  eventNames,
  topEventProperties,
  eventPropertyValues,
  profiles,
];
export const MAIN_TABLES: TableDescriptor[] = [funnelsList, cohorts, schemas, schemaProperties, annotations];
export const UNIFIED_TABLES: TableDescriptor[] = [
  unifiedEvents,
  unifiedUsers,
  unifiedCohorts,
  unifiedEventDefinitions,
  unifiedPropertyDefinitions,
];

export { DEFAULT_CHUNK_DAYS };
