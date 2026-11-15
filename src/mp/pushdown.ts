/**
 * Turn DuckDB's pushed-down WHERE predicates into Mixpanel request parameters.
 *
 * Every table function sets `autoApplyFilters: true`, so the SDK re-applies every
 * pushed predicate to each batch exactly. Translation therefore only has to NARROW
 * the upstream request and never drop a matching row: predicates that cannot be
 * expressed are not sent. Only top-level conjuncts are translated (a predicate
 * under OR / NOT is left to the SDK), so whatever is sent is implied by the SQL.
 */

import type { PushdownFilters } from "@query-farm/vgi";

type Expr = PushdownFilters["filters"][number];

/** One translatable constraint on a column. */
export type Constraint =
  | { kind: "cmp"; column: string; op: "eq" | "gt" | "ge" | "lt" | "le"; value: unknown }
  | { kind: "in"; column: string; values: unknown[] };

const FLIP: Record<string, "eq" | "gt" | "ge" | "lt" | "le"> = { eq: "eq", gt: "lt", ge: "le", lt: "gt", le: "ge" };

function unwrap(e: Expr): Expr {
  return e.node === "cast" ? unwrap(e.expression) : e;
}

function one(e: Expr, out: Constraint[]): void {
  if (e.node === "and") {
    for (const c of e.children) one(c, out);
    return;
  }
  if (e.node === "comparison" && e.op in FLIP) {
    const [l, r] = [unwrap(e.left), unwrap(e.right)];
    if (l.node === "column_ref" && r.node === "literal" && r.value != null) {
      out.push({ kind: "cmp", column: l.columnName, op: e.op as "eq", value: r.value });
    } else if (r.node === "column_ref" && l.node === "literal" && l.value != null) {
      out.push({ kind: "cmp", column: r.columnName, op: FLIP[e.op] ?? "eq", value: l.value });
    }
    return;
  }
  if (e.node === "in" && !e.negated && e.set.kind === "literal") {
    const col = unwrap(e.expression);
    const values = e.set.values.filter((v) => v != null);
    if (col.node === "column_ref" && values.length > 0 && values.length === e.set.values.length) {
      out.push({ kind: "in", column: col.columnName, values });
    }
  }
}

/** The top-level, translatable constraints of a scan's pushed filters. */
export function constraintsOf(filters: PushdownFilters | undefined): Constraint[] {
  const out: Constraint[] = [];
  for (const f of filters?.filters ?? []) one(f, out);
  return out;
}

export const DAY_MS = 86_400_000;

/** A pushed literal for a TIMESTAMPTZ / TIMESTAMP / DATE column as epoch ms. */
export function toEpochMs(v: unknown): number | undefined {
  if (typeof v === "bigint") return Number(v / 1000n); // microseconds
  if (typeof v === "number") return Math.abs(v) < 1e7 ? v * DAY_MS : v; // days or ms
  if (v instanceof Date) return v.getTime();
  if (typeof v === "string") {
    const t = Date.parse(/[zZ]|[+-]\d\d:?\d\d$/.test(v) || !/\d:\d/.test(v) ? v : `${v}Z`);
    return Number.isNaN(t) ? undefined : t;
  }
  return undefined;
}

/** The tightest inclusive [lower, upper] epoch-ms bounds the constraints put on one column. */
export function timeBounds(constraints: Constraint[], column: string): { lower?: number; upper?: number } {
  let lower: number | undefined;
  let upper: number | undefined;
  for (const c of constraints) {
    if (c.column !== column || c.kind !== "cmp") continue;
    const ms = toEpochMs(c.value);
    if (ms === undefined) continue;
    if (c.op === "gt" || c.op === "ge" || c.op === "eq") lower = lower === undefined ? ms : Math.max(lower, ms);
    if (c.op === "lt" || c.op === "le" || c.op === "eq") upper = upper === undefined ? ms : Math.min(upper, ms);
  }
  return { lower, upper };
}

/** Equality / IN values pushed for a column, intersected; undefined if unconstrained. */
export function eqValues(constraints: Constraint[], column: string): unknown[] | undefined {
  let set: unknown[] | undefined;
  for (const c of constraints) {
    if (c.column !== column) continue;
    let vs: unknown[] | undefined;
    if (c.kind === "cmp" && c.op === "eq") vs = [c.value];
    else if (c.kind === "in") vs = c.values;
    if (!vs) continue;
    const norm = vs.map((v) => (typeof v === "bigint" ? Number(v) : v));
    set = set ? set.filter((v) => norm.includes(v)) : [...new Set(norm)];
  }
  return set;
}

/** String equality / IN values for a column (only when every value is a string). */
export function eqStrings(constraints: Constraint[], column: string): string[] | undefined {
  const vs = eqValues(constraints, column);
  if (vs === undefined) return undefined;
  return vs.every((v) => typeof v === "string") ? (vs as string[]) : undefined;
}

// ------------------------------------------------------- Mixpanel expressions

/**
 * Quote a string literal for a Mixpanel segmentation expression: double quotes with
 * backslash escapes for `\` and `"` (and control characters escaped JSON-style).
 */
export function mpString(v: string): string {
  return JSON.stringify(v);
}

/** `properties["name"]` with the property name quoted safely. */
export function propRef(name: string, root = "properties"): string {
  return `${root}[${mpString(name)}]`;
}

/**
 * Build Mixpanel `where` conjuncts from string equality / IN constraints on columns
 * that map onto a property. IN lists become a parenthesised `or`. Only strings are
 * sent: Mixpanel's `==` is type-strict, so a string literal never matches a number and
 * the property's stored type is unknown.
 */
export function whereParts(
  constraints: Constraint[],
  props: Record<string, string>,
  ref: (prop: string) => string = (p) => propRef(p),
): string[] {
  const parts: string[] = [];
  for (const [column, prop] of Object.entries(props)) {
    const vs = eqStrings(constraints, column);
    if (vs === undefined) continue;
    if (vs.length === 0) {
      parts.push("false");
      continue;
    }
    if (vs.length > 50) continue;
    const r = ref(prop);
    const terms = vs.map((v) => `${r} == ${mpString(v)}`);
    parts.push(terms.length === 1 ? (terms[0] as string) : `(${terms.join(" or ")})`);
  }
  return parts;
}

/** AND together generated conjuncts and an optional caller-supplied expression. */
export function andJoin(parts: string[], userExpr?: string | null): string | undefined {
  const all = [...parts];
  const u = userExpr?.trim();
  if (u) all.unshift(parts.length ? `(${u})` : u);
  return all.length ? all.join(" and ") : undefined;
}
