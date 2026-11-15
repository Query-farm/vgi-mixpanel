/**
 * Typed columns over untyped Mixpanel JSON, and one factory for every
 * request-per-batch Mixpanel endpoint (Query API reports, Engage, cohorts, Lexicon,
 * annotations).
 *
 * A spec names its columns (each with a JSON extractor) and turns the call's
 * arguments plus the pushed WHERE predicates into a list of requests. The factory
 * builds a VGI table function that:
 *
 * - resolves the request list once, on the first call, and keeps it in serializable
 *   state with an index and the Engage paging cursor (`session_id` + page), so a scan
 *   resumes on any process over HTTP and survives rate-limit pauses;
 * - sends one request per batch;
 * - reads the `mixpanel` secret on every call and never stores it in state;
 * - pushes filters down (narrowing only) and lets the SDK re-apply them exactly.
 */

import {
  batchFromColumns,
  bool,
  constraintRecord,
  dateDay,
  defineTableFunction,
  field,
  float64,
  formatPushedFilters,
  int32,
  int64,
  list,
  schema,
  type TableDescriptor,
  timestampMicros,
  utf8,
  type VgiDataType,
  type VgiFunction,
} from "@query-farm/vgi";
import { log } from "../log.ts";
import { type Category, column, objectTags } from "../meta.ts";
import { configFromSecrets, MixpanelError, type MpConfig, type MpRequest, mpJson, SECRET_TYPE } from "./client.ts";
import { type Constraint, constraintsOf, DAY_MS } from "./pushdown.ts";

// ------------------------------------------------------------------ JSON → values

// biome-ignore lint/suspicious/noExplicitAny: Mixpanel JSON is untyped.
export type Row = Record<string, any> & { __ctx?: Record<string, unknown> };

/** Walk a dotted path through untyped JSON (`a.b`); a leading `=` reads one literal key. */
export function at(row: unknown, path: string): unknown {
  const parts = path.startsWith("=") ? [path.slice(1)] : path.split(".");
  let cur: unknown = row;
  for (const p of parts) {
    if (cur == null || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[p];
  }
  return cur;
}

export const asStr = (v: unknown): string | null =>
  v == null || v === "" ? null : typeof v === "string" ? v : typeof v === "object" ? JSON.stringify(v) : String(v);
export const asBool = (v: unknown): boolean | null =>
  typeof v === "boolean" ? v : v === 1 || v === "true" ? true : v === 0 || v === "false" ? false : null;
export function asNum(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "" && Number.isFinite(Number(v))) return Number(v);
  return null;
}
export const asInt = (v: unknown): number | null => {
  const n = asNum(v);
  return n === null ? null : Math.trunc(n);
};
export function asBig(v: unknown): bigint | null {
  if (typeof v === "bigint") return v;
  if (typeof v === "string" && /^-?\d+$/.test(v.trim())) return BigInt(v.trim());
  const n = asNum(v);
  return n === null ? null : BigInt(Math.trunc(n));
}
export const asStrList = (v: unknown): string[] | null =>
  Array.isArray(v) ? v.filter((x) => x != null).map((x) => (typeof x === "string" ? x : JSON.stringify(x))) : null;
export const asJson = (v: unknown): string | null => (v == null ? null : JSON.stringify(v));

const TIME_RE =
  /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?\s*(Z|[+-]\d{2}:?\d{2})?$/;

/**
 * A Mixpanel time string → microseconds since the epoch. Mixpanel writes times
 * without an offset (`2026-11-14T09:15:02`, `2026-11-14 09:15:02`, `2026-11-14`);
 * they are read as UTC. An explicit offset is honoured.
 */
export function parseTime(v: unknown): bigint | null {
  if (typeof v === "number" && Number.isFinite(v)) return epochToMicros(v);
  if (typeof v !== "string" || v === "") return null;
  const m = TIME_RE.exec(v.trim());
  if (!m) {
    const ms = Date.parse(v);
    return Number.isNaN(ms) ? null : BigInt(ms) * 1000n;
  }
  const [, y, mo, d, h = "0", mi = "0", s = "0", frac = "", tz] = m;
  let ms = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s));
  if (tz && tz !== "Z") {
    const sign = tz.startsWith("-") ? -1 : 1;
    const digits = tz.replace(/[+:-]/g, "");
    ms -= sign * (Number(digits.slice(0, 2)) * 60 + Number(digits.slice(2, 4))) * 60_000;
  }
  return BigInt(ms) * 1000n + BigInt(`${frac}000000`.slice(0, 6));
}

/** Epoch seconds or milliseconds (values past year 5000 in seconds are ms) → microseconds. */
export function epochToMicros(n: number): bigint | null {
  if (!Number.isFinite(n) || n <= 0) return null;
  return n > 95_000_000_000 ? BigInt(Math.round(n)) * 1000n : BigInt(Math.round(n * 1000)) * 1000n;
}

/** `YYYY-MM-DD` (or a timestamp string starting with one) → days since the epoch. */
export function dayNumber(v: unknown): number | null {
  if (typeof v !== "string") return null;
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(v);
  return m ? Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) / DAY_MS : null;
}

// --------------------------------------------------------------------- columns

export interface Col {
  name: string;
  type: VgiDataType;
  /** DuckDB spelling for vgi.result_columns_schema. */
  sqlType: string;
  doc: string;
  nullable: boolean;
  get: (row: Row) => unknown;
}

type Getter = string | ((r: Row) => unknown);
const read = (r: Row, g: Getter) => (typeof g === "string" ? at(r, g) : g(r));

function mk(
  name: string,
  type: VgiDataType,
  sqlType: string,
  doc: string,
  get: (r: Row) => unknown,
  nullable = true,
): Col {
  return { name, type, sqlType, doc, get, nullable };
}

const TSZ = "TIMESTAMP WITH TIME ZONE";

export const C = {
  str: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, utf8(), "VARCHAR", doc, (r) => asStr(read(r, g)), nullable),
  /** TIMESTAMPTZ from a Mixpanel time string or epoch seconds / ms. */
  time: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, timestampMicros("UTC"), TSZ, doc, (r) => parseTime(read(r, g)), nullable),
  /** A project-local bucket start (no time zone): TIMESTAMP. */
  localTime: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, timestampMicros(), "TIMESTAMP", doc, (r) => parseTime(read(r, g)), nullable),
  date: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, dateDay(), "DATE", doc, (r) => dayNumber(read(r, g)), nullable),
  bool: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, bool, "BOOLEAN", doc, (r) => asBool(read(r, g)), nullable),
  int: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, int32, "INTEGER", doc, (r) => asInt(read(r, g)), nullable),
  big: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, int64(), "BIGINT", doc, (r) => asBig(read(r, g)), nullable),
  num: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, float64(), "DOUBLE", doc, (r) => asNum(read(r, g)), nullable),
  strList: (name: string, doc: string, g: Getter) =>
    mk(name, list(utf8()), "VARCHAR[]", doc, (r) => asStrList(read(r, g))),
  json: (name: string, doc: string, g: Getter, nullable = true) =>
    mk(name, utf8(), "JSON", doc, (r) => asJson(read(r, g)), nullable),
  /** A constant (the `tool` column of the shared vocabulary). */
  lit: (name: string, doc: string, value: string) => mk(name, utf8(), "VARCHAR", doc, () => value, false),
};

const JSON_META: [string, string] = ["ARROW:extension:name", "arrow.json"];

/** The Arrow output schema of a column list (column comments ride along as field metadata). */
export function schemaOf(columns: Col[]) {
  return schema(
    columns.map((c) =>
      c.sqlType === "JSON"
        ? field(c.name, c.type, c.nullable, new Map([["comment", c.doc], JSON_META]))
        : column(c.name, c.type, c.doc, c.nullable),
    ),
  );
}

/** Build one batch of the bound output schema from rows. */
export function batchOf(columns: Col[], rows: Row[], outputSchema: ReturnType<typeof schema>) {
  const cols: Record<string, unknown[]> = {};
  for (const f of outputSchema.fields) {
    const c = columns.find((x) => x.name === f.name);
    cols[f.name] = c ? rows.map((r) => c.get(r)) : rows.map(() => null);
  }
  return batchFromColumns(cols, outputSchema);
}

// --------------------------------------------------------------------- arguments

/** A DATE / TIMESTAMP / string argument → Mixpanel's `YYYY-MM-DD`. */
export function ymd(v: unknown, argName: string): string {
  if (typeof v === "string") {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(v.trim());
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  }
  let ms: number | undefined;
  if (v instanceof Date) ms = v.getTime();
  else if (typeof v === "number") ms = Math.abs(v) < 1e7 ? v * DAY_MS : v;
  else if (typeof v === "bigint") ms = Number(v / 1000n);
  if (ms === undefined || Number.isNaN(ms)) {
    throw new MixpanelError(`${argName}: expected a DATE such as DATE '2026-11-01'`);
  }
  return new Date(ms).toISOString().slice(0, 10);
}

/** Days since the epoch → `YYYY-MM-DD`. */
export const dayString = (day: number): string => new Date(day * DAY_MS).toISOString().slice(0, 10);

/** Optional string argument: NULL / blank → undefined. */
export const optStr = (v: unknown): string | undefined =>
  typeof v === "string" && v.trim() !== "" ? v.trim() : undefined;

/** Optional integer argument (BIGINT or INTEGER): NULL → undefined. */
export const optInt = (v: unknown): number | undefined =>
  typeof v === "bigint" ? Number(v) : typeof v === "number" ? Math.trunc(v) : undefined;

// --------------------------------------------------------------------- the factory

export interface Req extends MpRequest {
  /** Values tagged onto every row of this request (`row.__ctx`). */
  ctx?: Record<string, unknown>;
  /** Engage paging: `session_id` + `page` form fields until a short page. */
  engage?: boolean;
}

export interface ScanContext<A> {
  args: A;
  constraints: Constraint[];
  cfg: MpConfig;
  /** Wall clock fixed when the scan started (epoch ms), stable across resumes. */
  anchorMs: number;
}

export interface FnExample {
  sql: string;
  description: string;
}

export interface MpSpec<A> {
  name: string;
  description: string;
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
  /** Text appended to doc_md describing what pushes down. */
  pushdown?: string;
  args?: Record<string, VgiDataType>;
  argDocs?: Record<string, string>;
  argDefaults?: Record<string, unknown>;
  argConstraints?: Record<string, Record<string, unknown>>;
  columns: Col[];
  /** The requests of one scan (resolved once, on the first call). */
  requests: (ctx: ScanContext<A>) => Req[] | Promise<Req[]>;
  /** Pull the rows out of a response body (default: the body when it is an array, else `body.results`). */
  rows?: (body: unknown, req: Req, ctx: ScanContext<A>) => Row[];
  examples: FnExample[];
}

interface ScanState {
  /** The resolved requests (JSON); "" until the first call. */
  reqs: string;
  idx: number;
  /** Engage cursor: the session id of page 0 and the next page number. */
  session: string;
  page: number;
  anchorMs: number;
}

export const DEFAULT_ROWS = (body: unknown): Row[] => {
  if (Array.isArray(body)) return body as Row[];
  const r = (body as { results?: unknown } | null)?.results;
  return Array.isArray(r) ? (r as Row[]) : [];
};

/** Discovery tags shared by a spec's function or table. */
export function tagsOf(spec: {
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
  pushdown?: string;
  columns: Col[];
}): Record<string, string> {
  return {
    domain: "product-analytics",
    ...objectTags({
      title: spec.title,
      category: spec.category,
      keywords: spec.keywords,
      docLlm: spec.docLlm,
      docMd: spec.docMd + (spec.pushdown ? `\n\n**Pushdown:** ${spec.pushdown}` : ""),
    }),
    "vgi.result_columns_schema": JSON.stringify(
      spec.columns.map((c) => ({ name: c.name, type: c.sqlType, description: c.doc })),
    ),
  };
}

export function defineMpTable<A extends Record<string, unknown>>(spec: MpSpec<A>): VgiFunction {
  const outSchema = schemaOf(spec.columns);
  const extract = spec.rows ?? DEFAULT_ROWS;
  return defineTableFunction<A, ScanState>({
    name: spec.name,
    description: spec.description,
    args: spec.args ?? {},
    argDocs: spec.argDocs ?? {},
    argDefaults: spec.argDefaults ?? {},
    argConstraints: spec.argConstraints as never,
    filterPushdown: true,
    // DuckDB drops pushed predicates from its plan; the SDK re-applies them exactly,
    // so translating a predicate only ever narrows what Mixpanel returns.
    autoApplyFilters: true,
    onBind: (params) =>
      params.resolvedSecretsProvided
        ? { outputSchema: outSchema }
        : { outputSchema: outSchema, lookupSecretTypes: [SECRET_TYPE] },
    initialState: () => ({ reqs: "", idx: 0, session: "", page: 0, anchorMs: Date.now() }),
    process: async (params, state, out) => {
      const cfg = configFromSecrets(params.secrets);
      const ctx: ScanContext<A> = {
        args: params.args,
        constraints: constraintsOf(params.pushdownFilters),
        cfg,
        anchorMs: state.anchorMs,
      };
      if (!state.reqs) {
        log.debug(`${spec.name}: pushed filters ${formatPushedFilters(params.pushdownFilters)}`);
        state.reqs = JSON.stringify(await spec.requests(ctx));
      }
      const reqs = JSON.parse(state.reqs) as Req[];
      // Loop until a request yields rows or the scan ends, so no empty batches go out.
      for (;;) {
        const req = reqs[state.idx];
        if (!req) {
          out.finish();
          return;
        }
        let send: Req = req;
        if (req.engage && state.page > 0) {
          send = { ...req, form: { ...(req.form ?? {}), session_id: state.session, page: state.page } };
        }
        const body = await mpJson(cfg, send);
        const rows = extract(body, req, ctx);
        let advance = true;
        if (req.engage) {
          const b = (body ?? {}) as { session_id?: string; page_size?: number };
          const pageSize = typeof b.page_size === "number" && b.page_size > 0 ? b.page_size : 1000;
          if (rows.length >= pageSize && b.session_id) {
            state.session = b.session_id;
            state.page += 1;
            advance = false;
          }
        }
        if (advance) {
          state.idx += 1;
          state.session = "";
          state.page = 0;
        }
        if (rows.length === 0) continue;
        const tagged = req.ctx ? rows.map((r) => ({ ...r, __ctx: req.ctx })) : rows;
        out.emit(batchOf(spec.columns, tagged, params.outputSchema));
        return;
      }
    },
    tags: tagsOf(spec),
    examples: spec.examples,
  });
}

/** A table descriptor over a scan function, with NOT NULL / PRIMARY KEY attestations. */
export function catalogTable(
  spec: {
    name: string;
    description: string;
    title: string;
    category: Category;
    keywords: string;
    docLlm: string;
    docMd: string;
    pushdown?: string;
    columns: Col[];
    examples: FnExample[];
  },
  fn: VgiFunction,
  schemaPath: string[],
  primaryKey: string[],
): TableDescriptor {
  const record = (id: string, kind: "NOT_NULL" | "PRIMARY_KEY", columns: string[]) =>
    constraintRecord({
      schemaPath,
      table: spec.name,
      id,
      kind,
      columns: columns.map((c) => [c]),
      enforcement: "ENFORCED",
      validation: "ALL_ROWS",
    });
  const notNull = spec.columns.filter((c) => !c.nullable).map((c) => c.name);
  return {
    name: spec.name,
    comment: spec.description,
    columns: schemaOf(spec.columns),
    function: fn,
    constraints: [
      ...notNull.map((c) => record(`${c}_not_null`, "NOT_NULL", [c])),
      ...(primaryKey.length ? [record("pk", "PRIMARY_KEY", primaryKey)] : []),
    ],
    tags: { ...tagsOf(spec), "vgi.example_queries": JSON.stringify(spec.examples) },
  };
}

/** A spec with no arguments exposed as a catalog table (`SELECT … FROM mixpanel.main.cohorts`). */
export function defineMpCatalogTable(
  spec: MpSpec<Record<string, never>>,
  schemaPath: string[],
  primaryKey: string[],
): TableDescriptor {
  const fn = defineMpTable({ ...spec, name: `${schemaPath.join("_")}_${spec.name}_scan` });
  return catalogTable(spec, fn, schemaPath, primaryKey);
}
