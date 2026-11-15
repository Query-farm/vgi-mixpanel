// Unit tests. The SQL-level tests live in test/sql and run through DuckDB.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { CATALOG } from "../src/catalog.ts";
import { configFromSecrets, type MpConfig, mpJson, retryDelayMs, urlOf } from "../src/mp/client.ts";
import {
  closeAllStreams,
  EVENT_PROPS,
  type ExportCursor,
  exportStep,
  parseLine,
  planExport,
} from "../src/mp/export.ts";
import { andJoin, type Constraint, mpString, propRef, timeBounds, whereParts } from "../src/mp/pushdown.ts";
import { engageForm, funnelRows, propertyRows, retentionRows, segmentationRows } from "../src/mp/resources.ts";
import { parseTime } from "../src/mp/table.ts";

const ROOT = join(import.meta.dir, "..");
const DAY = 86_400_000;
const SECRET = { type: "mixpanel", username: "sa.test", secret: "test-secret", project_id: "4242" };

test("configFromSecrets: credentials, project id and region are validated", () => {
  expect(() => configFromSecrets({})).toThrow(/no mixpanel secret/);
  expect(() => configFromSecrets({ s: { type: "mixpanel", username: "u", secret: "s" } })).toThrow(/project_id/);
  expect(() => configFromSecrets({ s: { ...SECRET, project_id: "abc" } })).toThrow(/numeric/);
  expect(() => configFromSecrets({ s: { ...SECRET, region: "apac" } })).toThrow(/region/);
  expect(() => configFromSecrets({ s: { type: "mixpanel", project_id: "1" } })).toThrow(/USERNAME and SECRET/);
  expect(() => configFromSecrets({ s: { ...SECRET, base_url: "http://evil.example.com" } })).toThrow(/https/);
  const cfg = configFromSecrets({ s: SECRET });
  expect(cfg.auth).toBe(`Basic ${Buffer.from("sa.test:test-secret").toString("base64")}`);
  expect(cfg.region).toBe("us");
  const legacy = configFromSecrets({ s: { type: "mixpanel", api_secret: "abc", project_id: "7", region: "EU" } });
  expect(legacy.auth).toBe(`Basic ${Buffer.from("abc:").toString("base64")}`);
  expect(legacy.region).toBe("eu");
});

test("urlOf: region hosts per API family; project_id on Query/export calls only", () => {
  const eu = configFromSecrets({ s: { ...SECRET, region: "eu" } });
  expect(urlOf(eu, { family: "export", path: "/api/2.0/export", query: { from_date: "2026-11-01" } })).toBe(
    "https://data-eu.mixpanel.com/api/2.0/export?project_id=4242&from_date=2026-11-01",
  );
  const india = configFromSecrets({ s: { ...SECRET, region: "in", workspace_id: "9" } });
  expect(urlOf(india, { family: "query", path: "/api/query/engage" })).toBe(
    "https://in.mixpanel.com/api/query/engage?project_id=4242&workspace_id=9",
  );
  const us = configFromSecrets({ s: SECRET });
  expect(urlOf(us, { family: "app", path: "/api/app/projects/{project}/schemas" })).toBe(
    "https://mixpanel.com/api/app/projects/4242/schemas",
  );
  const local = configFromSecrets({ s: { ...SECRET, base_url: "http://127.0.0.1:9/" } });
  expect(urlOf(local, { family: "export", path: "/api/2.0/export" })).toBe(
    "http://127.0.0.1:9/api/2.0/export?project_id=4242",
  );
});

test("retryDelayMs: Retry-After seconds or date, else exponential, capped", () => {
  expect(retryDelayMs(new Headers({ "Retry-After": "3" }), 1)).toBe(3000);
  expect(retryDelayMs(new Headers({ "Retry-After": "3600" }), 1)).toBe(60_000);
  expect(retryDelayMs(null, 1)).toBe(1000);
  expect(retryDelayMs(null, 3)).toBe(4000);
  const soon = new Date(Date.now() + 5000).toUTCString();
  expect(retryDelayMs(new Headers({ "Retry-After": soon }), 1)).toBeGreaterThan(3000);
});

test("expressions: string literals and property names are quoted and escaped", () => {
  expect(mpString('O"Fallon \\ x')).toBe('"O\\"Fallon \\\\ x"');
  expect(propRef('we"ird')).toBe('properties["we\\"ird"]');
  const cs: Constraint[] = [
    { kind: "cmp", column: "os", op: "eq", value: "iOS" },
    { kind: "in", column: "city", values: ["A", "B"] },
    { kind: "cmp", column: "browser", op: "gt", value: "x" },
    { kind: "cmp", column: "unmapped", op: "eq", value: "z" },
    { kind: "cmp", column: "region", op: "eq", value: 5 },
  ];
  const parts = whereParts(cs, { os: "$os", city: "$city", browser: "$browser", region: "$region" });
  expect(parts).toEqual(['properties["$os"] == "iOS"', '(properties["$city"] == "A" or properties["$city"] == "B")']);
  expect(andJoin(parts.slice(0, 1), 'properties["x"] == 1')).toBe(
    '(properties["x"] == 1) and properties["$os"] == "iOS"',
  );
  expect(andJoin([], "  ")).toBeUndefined();
  // An empty intersection of equality filters sends `false` rather than widening.
  const none: Constraint[] = [
    { kind: "cmp", column: "os", op: "eq", value: "iOS" },
    { kind: "cmp", column: "os", op: "eq", value: "Android" },
  ];
  expect(whereParts(none, { os: "$os" })).toEqual(["false"]);
});

test("timeBounds: inclusive lower/upper from comparisons (µs, ms, ISO)", () => {
  const lo = Date.UTC(2026, 10, 2);
  const hi = Date.UTC(2026, 10, 4);
  const cs: Constraint[] = [
    { kind: "cmp", column: "event_time", op: "ge", value: BigInt(lo) * 1000n },
    { kind: "cmp", column: "event_time", op: "lt", value: "2026-11-04T00:00:00Z" },
    { kind: "cmp", column: "other", op: "lt", value: 0 },
  ];
  expect(timeBounds(cs, "event_time")).toEqual({ lower: lo, upper: hi });
});

describe("planExport", () => {
  const anchor = Date.UTC(2026, 10, 15, 12);
  const day = (iso: string) => Date.parse(`${iso}T00:00:00Z`) / DAY;

  test("default window: the last lookback_days up to today", () => {
    const p = planExport({}, [], EVENT_PROPS, anchor);
    expect([p.fromDay, p.toDay]).toEqual([day("2026-11-09"), day("2026-11-15")]);
    expect(p.event).toBe("");
    expect(p.where).toBe("");
  });

  test("event_time bounds widen by a day each side; upper clamps to today", () => {
    const cs: Constraint[] = [
      { kind: "cmp", column: "event_time", op: "ge", value: "2026-11-02T00:00:00Z" },
      { kind: "cmp", column: "event_time", op: "lt", value: "2026-12-30T00:00:00Z" },
      { kind: "in", column: "event_name", values: ["A", "B"] },
      { kind: "cmp", column: "distinct_id", op: "eq", value: "u1" },
    ];
    const p = planExport({}, cs, EVENT_PROPS, anchor);
    expect([p.fromDay, p.toDay]).toEqual([day("2026-11-01"), day("2026-11-15")]);
    expect(p.event).toBe('["A","B"]');
    expect(p.where).toBe('string(properties["distinct_id"]) == "u1"');
  });

  test("explicit dates win, intersected with pushed bounds; empty name set reads nothing", () => {
    const cs: Constraint[] = [{ kind: "cmp", column: "event_time", op: "ge", value: "2026-11-03T05:00:00Z" }];
    const p = planExport({ from_date: "2026-11-01", to_date: "2026-11-05" }, cs, EVENT_PROPS, anchor);
    expect([p.fromDay, p.toDay]).toEqual([day("2026-11-02"), day("2026-11-05")]);
    const none: Constraint[] = [
      { kind: "cmp", column: "event_name", op: "eq", value: "A" },
      { kind: "cmp", column: "event_name", op: "eq", value: "B" },
    ];
    expect(planExport({}, none, EVENT_PROPS, anchor).empty).not.toBe("");
  });
});

test("parseLine: event + properties; error lines raise", () => {
  const r = parseLine('{"event":"Sign Up","properties":{"time":1,"distinct_id":"u1"}}');
  expect(r?.__name).toBe("Sign Up");
  expect(r?.p.distinct_id).toBe("u1");
  expect(() => parseLine("not json")).toThrow(/not JSON/);
  expect(() => parseLine('{"error":"bad where"}')).toThrow(/bad where/);
});

test("parseTime: Mixpanel strings are UTC; offsets honoured; epoch seconds and ms", () => {
  expect(parseTime("2026-11-01T09:00:00")).toBe(BigInt(Date.UTC(2026, 10, 1, 9)) * 1000n);
  expect(parseTime("2026-11-01 09:00:00")).toBe(BigInt(Date.UTC(2026, 10, 1, 9)) * 1000n);
  expect(parseTime("2026-11-01T09:00:00-05:00")).toBe(BigInt(Date.UTC(2026, 10, 1, 14)) * 1000n);
  expect(parseTime(1_793_523_600)).toBe(1_793_523_600_000_000n);
  expect(parseTime(1_793_523_600_000)).toBe(1_793_523_600_000_000n);
  expect(parseTime("")).toBeNull();
});

test("report shapes → long rows", () => {
  const seg = { data: { series: ["d1", "d2"], values: { iOS: { d2: 2, d1: 1 }, Android: { d1: 0, d2: 3 } } } };
  expect(segmentationRows(seg, true, "Sign Up").map((r) => [r.segment, r.period, r.value])).toEqual([
    ["Android", "d1", 0],
    ["Android", "d2", 3],
    ["iOS", "d1", 1],
    ["iOS", "d2", 2],
  ]);
  expect(segmentationRows({ data: { values: { "Sign Up": { d1: 4 } } } }, false, "Sign Up")[0]).toEqual({
    event: "Sign Up",
    segment: null,
    period: "d1",
    value: 4,
  });
  const fun = { data: { "2026-11-02": { steps: [{ count: 4 }, { count: 2 }], analysis: {} } } };
  expect(funnelRows(fun).map((r) => [r.period, r.step, r.step_count, r.count])).toEqual([
    ["2026-11-02", 1, 2, 4],
    ["2026-11-02", 2, 2, 2],
  ]);
  const ret = retentionRows({ "2026-11-01": { counts: [4, 1], first: 4 } });
  expect(ret.map((r) => [r.period, r.retained, r.rate])).toEqual([
    [0, 4, 1],
    [1, 1, 0.25],
  ]);
  expect(retentionRows({ "2026-11-01": { counts: [0], first: 0 } })[0]?.rate).toBeNull();
  const props = propertyRows({
    results: [
      {
        entityType: "event",
        name: "E",
        schemaJson: {
          required: ["b"],
          properties: { b: { type: ["integer", "null"] }, a: { type: "string", enum: [1] } },
        },
      },
    ],
  });
  expect(props.map((p) => [p.property_name, p.data_type, p.is_required, p.enum_values])).toEqual([
    ["a", "string", false, [1]],
    ["b", "integer", true, null],
  ]);
});

test("engageForm: distinct ids, where, cohort", () => {
  const cs: Constraint[] = [
    { kind: "in", column: "distinct_id", values: ["a", "b"] },
    { kind: "cmp", column: "email", op: "eq", value: "x@y" },
  ];
  const { form, empty } = engageForm(cs, { email: "$email" }, ["distinct_id"], 'properties["p"] == 1', 5);
  expect(empty).toBe(false);
  expect(form).toEqual({
    where: '(properties["p"] == 1) and string(properties["$email"]) == "x@y"',
    distinct_ids: '["a","b"]',
    filter_by_cohort: '{"id":5}',
  });
  const one = engageForm([{ kind: "cmp", column: "user_id", op: "eq", value: "a" }], {}, ["user_id", "distinct_id"]);
  expect(one.form.distinct_id).toBe("a");
  const clash = engageForm(
    [
      { kind: "cmp", column: "user_id", op: "eq", value: "a" },
      { kind: "cmp", column: "distinct_id", op: "eq", value: "b" },
    ],
    {},
    ["user_id", "distinct_id"],
  );
  expect(clash.empty).toBe(true);
});

test("catalog: main + unified schemas, mixpanel secret type with redacted secrets", () => {
  expect(CATALOG.name).toBe("mixpanel");
  expect(CATALOG.schemas.map((s) => (s.path ?? []).join("."))).toEqual(["main", "unified"]);
  const secret = CATALOG.secretTypes?.[0];
  expect(secret?.name).toBe("mixpanel");
  const redacted = secret?.schema.fields.filter((f) => f.metadata.get("redact") === "true").map((f) => f.name);
  expect(redacted).toEqual(["secret", "api_secret"]);
});

describe("against the replay server", () => {
  let mock: ReturnType<typeof Bun.spawn>;
  let cfg: MpConfig;

  beforeAll(async () => {
    mock = Bun.spawn([process.env.PYTHON ?? "python3", join(ROOT, "test", "mock_api", "server.py")], {
      stdout: "pipe",
      stderr: "inherit",
    });
    const reader = (mock.stdout as ReadableStream<Uint8Array>).getReader();
    let text = "";
    while (!text.includes("\n")) {
      const { value, done } = await reader.read();
      if (done) break;
      text += new TextDecoder().decode(value);
    }
    reader.releaseLock();
    const port = text.match(/^PORT:(\d+)/)?.[1];
    if (!port) throw new Error(`replay server did not report a port: ${text}`);
    cfg = configFromSecrets({ s: { ...SECRET, base_url: `http://127.0.0.1:${port}` } });
  });

  afterAll(() => {
    closeAllStreams();
    mock?.kill();
  });

  const cursor = (): ExportCursor => {
    const from = Date.parse("2026-11-01T00:00:00Z") / DAY;
    return { fromDay: from, toDay: from + 2, chunk: from, consumed: 0, event: "", where: "", token: randomUUID() };
  };

  async function drain(state: ExportCursor, chunkDays: number, batch: number, forget: boolean): Promise<string[]> {
    const ids: string[] = [];
    for (;;) {
      if (forget) closeAllStreams(); // as if each batch were served by another process
      const rows = await exportStep(cfg, state, chunkDays, batch);
      if (rows === null) return ids;
      expect(rows.length).toBeGreaterThan(0);
      expect(rows.length).toBeLessThanOrEqual(batch);
      ids.push(...rows.map((r) => String(r.p.$insert_id)));
    }
  }

  test("export streams in small batches and resumes from the cursor in a fresh process", async () => {
    const warm = await drain(cursor(), 3, 2, false);
    expect(warm).toEqual(["e01", "e02", "e03", "e04", "e05", "e06", "e07", "e09", "e08", "e10", "e11", "e12", "e13"]);
    const cold = await drain(cursor(), 3, 2, true);
    expect(cold).toEqual(warm);
    const daily = await drain(cursor(), 1, 3, true);
    expect(daily).toEqual(warm);
  });

  test("errors carry the HTTP status and Mixpanel's message", async () => {
    await expect(
      mpJson(cfg, { family: "query", path: "/api/query/funnels", query: { from_date: "x" } }),
    ).rejects.toThrow(/HTTP 400.*funnel_id is a required/);
    const bad = { ...cfg, auth: "Basic eDp5" };
    await expect(mpJson(bad, { family: "query", path: "/api/query/events/names" })).rejects.toThrow(
      /HTTP 401.*check the mixpanel secret/,
    );
  });
});
