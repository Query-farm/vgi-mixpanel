#!/usr/bin/env python3
"""An offline, strict Mixpanel API replay server for the tests (stdlib only).

It serves the hand-built project in ``test/fixtures/mixpanel-project.json`` using the
response shapes documented at https://docs.mixpanel.com/reference/ and behaves like
Mixpanel where the worker depends on it:

* **Auth.** HTTP Basic with the fixture service account (``sa.test:test-secret``) or
  the legacy project secret (``legacy-secret:``). Anything else is a 401
  ``{"error": ..., "status": "error"}``. Query and export calls must carry
  ``project_id=4242`` (403 otherwise); App API paths must name project 4242.
* **Strict parameters.** Each route accepts only its documented parameters; an
  unknown one, a bad date, ``from_date > to_date`` or a ``to_date`` after today (UTC)
  is a 400.
* **Expressions.** ``where`` / ``born_where`` / ``on`` are parsed and evaluated
  (``properties["x"]``, string/number/boolean literals with JSON escapes, ``== != <
  <= > >=``, ``in``, ``and or not``, parentheses, ``defined() string() number()
  boolean()``; ``==`` is type-strict, as in Mixpanel), so pushdown really narrows the
  data. An unparsable expression is a 400.
* **Raw export.** NDJSON streamed line by line with chunked transfer encoding;
  ``event`` JSON array and ``where`` filters; ``time`` in seconds, or ms with
  ``time_in_ms=true``. ``recent_events`` are placed ``days_ago`` before today so the
  default lookback window always has data.
* **Engage.** Pages of ``engage_page_size`` (2) profiles; a page > 0 needs the
  ``session_id`` issued by page 0. ``distinct_id``, ``distinct_ids``, ``where`` and
  ``filter_by_cohort`` (members without a profile come back with only a distinct_id).
* **Reports.** Segmentation is computed from the fixture events (general / unique /
  average, day / hour / month units, ``on`` segmentation, ``where``); top property and
  value lists are computed too; funnels and retention replay recorded shapes.
* **Rate limits.** The first ``/api/query/events/top`` request answers 429 with
  ``Retry-After: 1`` and the first ``/api/query/funnels/list`` request answers 503, so
  the retry paths always run. ``/api/query/events/names?type=average`` always answers
  429 (an exhausted hourly budget).
* **Introspection.** ``GET /__requests?path=/api/query/engage`` returns every recorded
  request to that path (``seq``, ``method``, ``path``, ``query`` and ``form`` as JSON
  object strings, ``user``), so SQL tests can assert exactly what was pushed down.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import re
import sys
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
PROJECT = json.loads((HERE.parent / "fixtures" / "mixpanel-project.json").read_text())
PROJECT_ID = str(PROJECT["project_id"])
SA = PROJECT["service_account"]
VALID_AUTH = {
    "Basic " + base64.b64encode(f"{SA['username']}:{SA['secret']}".encode()).decode(): SA["username"],
    "Basic " + base64.b64encode(f"{PROJECT['api_secret']}:".encode()).decode(): "api_secret",
}
LOCK = threading.Lock()
REQUESTS: list[dict] = []
ONCE: set[str] = set()
SESSIONS: dict[str, str] = {}


class HttpError(Exception):
    def __init__(self, status: int, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status = status
        self.headers = headers or {}


def today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def parse_iso(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def all_events() -> list[dict]:
    """Fixture events with `time` resolved to an aware datetime (recent ones relative to today)."""
    out = []
    for e in PROJECT["events"]:
        out.append({"event": e["event"], "t": parse_iso(e["time"]), "properties": e["properties"]})
    base = dt.datetime.combine(today(), dt.time(0), dt.timezone.utc)
    for e in PROJECT["recent_events"]:
        t = base - dt.timedelta(days=e["days_ago"]) + dt.timedelta(hours=e["hour"])
        out.append({"event": e["event"], "t": t, "properties": e["properties"]})
    out.sort(key=lambda e: e["t"])
    return out


# ------------------------------------------------------------------ expressions

UNDEF = object()
TOKEN = re.compile(
    r'\s*(?:(?P<str>"(?:[^"\\]|\\.)*")|(?P<num>-?\d+(?:\.\d+)?)|(?P<op>==|!=|>=|<=|>|<|\(|\)|\[|\]|,)'
    r"|(?P<id>[A-Za-z_$][A-Za-z0-9_$]*))"
)


def tokenize(src: str) -> list[tuple[str, object]]:
    pos, out = 0, []
    while pos < len(src):
        if src[pos:].strip() == "":
            break
        m = TOKEN.match(src, pos)
        if not m or m.end() == pos:
            raise HttpError(400, f"invalid expression near: {src[pos:pos + 20]!r}")
        pos = m.end()
        if m.group("str") is not None:
            out.append(("str", json.loads(m.group("str"))))
        elif m.group("num") is not None:
            n = m.group("num")
            out.append(("num", float(n) if "." in n else int(n)))
        elif m.group("op") is not None:
            out.append(("op", m.group("op")))
        else:
            out.append(("id", m.group("id")))
    return out


class Parser:
    """Recursive descent over Mixpanel's segmentation expression grammar (the subset used)."""

    def __init__(self, src: str):
        self.toks = tokenize(src)
        self.i = 0

    def peek(self):
        return self.toks[self.i] if self.i < len(self.toks) else (None, None)

    def take(self, kind=None, value=None):
        k, v = self.peek()
        if k is None or (kind and k != kind) or (value is not None and v != value):
            raise HttpError(400, f"invalid expression: expected {value or kind}, got {v!r}")
        self.i += 1
        return v

    def parse(self):
        node = self.or_()
        if self.i != len(self.toks):
            raise HttpError(400, f"invalid expression: unexpected {self.peek()[1]!r}")
        return node

    def or_(self):
        node = self.and_()
        while self.peek() == ("id", "or"):
            self.take()
            node = ("or", node, self.and_())
        return node

    def and_(self):
        node = self.not_()
        while self.peek() == ("id", "and"):
            self.take()
            node = ("and", node, self.not_())
        return node

    def not_(self):
        if self.peek() == ("id", "not"):
            self.take()
            return ("not", self.not_())
        return self.cmp()

    def cmp(self):
        left = self.primary()
        k, v = self.peek()
        if k == "op" and v in ("==", "!=", ">", ">=", "<", "<="):
            self.take()
            return ("cmp", v, left, self.primary())
        if (k, v) == ("id", "in"):
            self.take()
            return ("in", left, self.primary())
        return left

    def primary(self):
        k, v = self.peek()
        if k in ("str", "num"):
            self.take()
            return ("lit", v)
        if k == "op" and v == "(":
            self.take()
            node = self.or_()
            self.take("op", ")")
            return node
        if k == "op" and v == "[":
            self.take()
            items = []
            while self.peek() != ("op", "]"):
                items.append(self.primary())
                if self.peek() == ("op", ","):
                    self.take()
            self.take("op", "]")
            return ("list", items)
        if k == "id" and v in ("true", "false"):
            self.take()
            return ("lit", v == "true")
        if k == "id" and v in ("properties", "user", "event"):
            self.take()
            self.take("op", "[")
            name = self.take("str")
            self.take("op", "]")
            return ("prop", name)
        if k == "id" and v in ("defined", "string", "number", "boolean"):
            self.take()
            self.take("op", "(")
            arg = self.or_()
            self.take("op", ")")
            return ("fn", v, arg)
        raise HttpError(400, f"invalid expression: unexpected {v!r}")


def compile_expr(src: str | None):
    if not src:
        return None
    return Parser(src).parse()


def evaluate(node, props: dict):
    kind = node[0]
    if kind == "lit":
        return node[1]
    if kind == "list":
        return [evaluate(n, props) for n in node[1]]
    if kind == "prop":
        return props.get(node[1], UNDEF)
    if kind == "fn":
        v = evaluate(node[2], props)
        if node[1] == "defined":
            return v is not UNDEF and v is not None
        if v is UNDEF or v is None:
            return UNDEF
        if node[1] == "string":
            if isinstance(v, bool):
                return "true" if v else "false"
            if isinstance(v, float) and v.is_integer():
                return str(int(v))
            return v if isinstance(v, str) else json.dumps(v) if isinstance(v, (list, dict)) else str(v)
        if node[1] == "number":
            try:
                return float(v)
            except (TypeError, ValueError):
                return UNDEF
        return v in (True, "true", 1) or (isinstance(v, str) and v.lower() == "true")
    if kind == "not":
        v = evaluate(node[1], props)
        return True if v is UNDEF else not bool(v)
    if kind in ("and", "or"):
        a = evaluate(node[1], props)
        a = False if a is UNDEF else bool(a)
        if kind == "and" and not a:
            return False
        if kind == "or" and a:
            return True
        b = evaluate(node[2], props)
        return False if b is UNDEF else bool(b)
    if kind == "in":
        a, b = evaluate(node[1], props), evaluate(node[2], props)
        if isinstance(b, list):
            return a in b
        return isinstance(a, str) and isinstance(b, str) and a in b
    if kind == "cmp":
        op, a, b = node[1], evaluate(node[2], props), evaluate(node[3], props)
        if a is UNDEF or b is UNDEF:
            return op == "!="
        num = (int, float)
        same = type(a) is type(b) or (isinstance(a, num) and isinstance(b, num) and not isinstance(a, bool))
        if op == "==":
            return same and a == b
        if op == "!=":
            return not (same and a == b)
        if not same:
            return False
        return {">": a > b, ">=": a >= b, "<": a < b, "<=": a <= b}[op]
    raise HttpError(400, "invalid expression")


def matches(node, props: dict) -> bool:
    if node is None:
        return True
    v = evaluate(node, props)
    return v is not UNDEF and bool(v)


# ------------------------------------------------------------------ helpers


def parse_date(s: str | None, name: str, required: bool = True) -> dt.date | None:
    if s is None:
        if required:
            raise HttpError(400, f"{name} is a required parameter")
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        raise HttpError(400, f"{name}: invalid date, expected yyyy-mm-dd")
    try:
        return dt.date.fromisoformat(s)
    except ValueError as err:
        raise HttpError(400, f"{name}: {err}") from err


def date_range(q: dict, max_today: bool = True) -> tuple[dt.date, dt.date]:
    f, t = parse_date(q.get("from_date"), "from_date"), parse_date(q.get("to_date"), "to_date")
    assert f and t
    if f > t:
        raise HttpError(400, "from_date must be before or equal to to_date")
    if max_today and t > today():
        raise HttpError(400, "to_date cannot be later than today")
    return f, t


def int_param(q: dict, name: str, default: int | None = None, lo: int = 1, hi: int = 1_000_000) -> int | None:
    v = q.get(name)
    if v is None:
        return default
    if not re.fullmatch(r"-?\d+", v) or not lo <= int(v) <= hi:
        raise HttpError(400, f"{name}: must be an integer between {lo} and {hi}")
    return int(v)


def choice(q: dict, name: str, choices: tuple[str, ...], default: str | None) -> str | None:
    v = q.get(name, default)
    if v is not None and v not in choices:
        raise HttpError(400, f"{name}: must be one of {', '.join(choices)}")
    return v


def bucket(t: dt.datetime, unit: str) -> str:
    if unit == "minute":
        return t.strftime("%Y-%m-%d %H:%M:00")
    if unit == "hour":
        return t.strftime("%Y-%m-%d %H:00:00")
    if unit == "month":
        return t.strftime("%Y-%m-01")
    if unit == "week":
        return (t.date() - dt.timedelta(days=t.weekday())).isoformat()
    return t.date().isoformat()


def buckets_between(f: dt.date, t: dt.date, unit: str) -> list[str]:
    out: list[str] = []
    cur = dt.datetime.combine(f, dt.time(0), dt.timezone.utc)
    end = dt.datetime.combine(t, dt.time(23, 59), dt.timezone.utc)
    step = {"minute": dt.timedelta(minutes=1), "hour": dt.timedelta(hours=1)}.get(unit, dt.timedelta(days=1))
    while cur <= end:
        b = bucket(cur, unit)
        if not out or out[-1] != b:
            out.append(b)
        cur += step
    return out


def events_in(f: dt.date, t: dt.date) -> list[dict]:
    return [e for e in all_events() if f <= e["t"].date() <= t]


# ------------------------------------------------------------------ routes

ROUTES: dict[tuple[str, str], tuple[set[str], object]] = {}


def route(method: str, path: str, params: set[str]):
    def wrap(fn):
        ROUTES[(method, path)] = (params | {"project_id", "workspace_id"}, fn)
        return fn

    return wrap


@route("GET", "/api/2.0/export", {"from_date", "to_date", "event", "where", "limit", "time_in_ms"})
def export(h, q, form):
    f, t = date_range(q)
    names = None
    if "event" in q:
        try:
            names = json.loads(q["event"])
        except ValueError as err:
            raise HttpError(400, "event: must be a JSON array of event names") from err
        if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
            raise HttpError(400, "event: must be a JSON array of event names")
    where = compile_expr(q.get("where"))
    limit = int_param(q, "limit", None, 1, 100_000)
    ms = choice(q, "time_in_ms", ("true", "false"), "false") == "true"
    rows = []
    for e in events_in(f, t):
        if names is not None and e["event"] not in names:
            continue
        if not matches(where, e["properties"]):
            continue
        epoch_ms = int(e["t"].timestamp() * 1000)
        props = {"time": epoch_ms if ms else epoch_ms // 1000, **e["properties"], "mp_processing_time_ms": epoch_ms + 2000}
        rows.append(json.dumps({"event": e["event"], "properties": props}))
        if limit and len(rows) >= limit:
            break
    h.send_response(200)
    h.send_header("Content-Type", "text/plain; charset=utf-8")
    h.send_header("Transfer-Encoding", "chunked")
    h.end_headers()
    for line in rows:  # one chunk per line: a genuinely incremental stream
        data = (line + "\n").encode()
        h.wfile.write(f"{len(data):X}\r\n".encode() + data + b"\r\n")
        h.wfile.flush()
    h.wfile.write(b"0\r\n\r\n")
    return None


@route("GET", "/api/query/segmentation", {"event", "from_date", "to_date", "on", "unit", "interval", "where", "limit", "type", "format"})
def segmentation(h, q, form):
    if not q.get("event"):
        raise HttpError(400, "event is a required parameter")
    f, t = date_range(q)
    unit = choice(q, "unit", ("minute", "hour", "day", "week", "month"), "day")
    typ = choice(q, "type", ("general", "unique", "average"), "general")
    where = compile_expr(q.get("where"))
    on = compile_expr(q.get("on"))
    limit = int_param(q, "limit", 60, 1, 10_000)
    series = buckets_between(f, t, unit)
    groups: dict[str, dict[str, list]] = {}
    for e in events_in(f, t):
        if e["event"] != q["event"] or not matches(where, e["properties"]):
            continue
        if on is not None:
            v = evaluate(on, e["properties"])
            if v is UNDEF or v is None:
                continue
            key = v if isinstance(v, str) else json.dumps(v)
        else:
            key = q["event"]
        groups.setdefault(key, {}).setdefault(bucket(e["t"], unit), []).append(str(e["properties"].get("distinct_id")))
    if on is None and not groups:
        groups[q["event"]] = {}
    keys = sorted(groups, key=lambda k: -sum(len(v) for v in groups[k].values()))[: (limit if on is not None else None)]
    values = {}
    for k in keys:
        out = {}
        for b in series:
            ids = groups[k].get(b, [])
            n = {"general": len(ids), "unique": len(set(ids))}.get(typ)
            if typ == "average":
                n = len(ids) / len(set(ids)) if ids else 0
            out[b] = n
        values[k] = out
    return {"data": {"series": series, "values": values}, "legend_size": len(values)}


@route("GET", "/api/query/funnels/list", set())
def funnels_list(h, q, form):
    with LOCK:
        first = "funnels_list" not in ONCE
        ONCE.add("funnels_list")
    if first:
        raise HttpError(503, "service temporarily unavailable")
    return PROJECT["funnels"]["list"]


@route("GET", "/api/query/funnels", {"funnel_id", "from_date", "to_date", "length", "length_unit", "interval", "unit", "on", "where", "limit"})
def funnels(h, q, form):
    fid = q.get("funnel_id")
    if fid is None:
        raise HttpError(400, "funnel_id is a required parameter")
    f, t = date_range(q)
    unit = choice(q, "unit", ("day", "week", "month"), "day")
    choice(q, "length_unit", ("day", "hour", "minute", "second"), None)
    int_param(q, "length", None, 1, 90 * 86400)
    compile_expr(q.get("where"))
    if "on" in q:
        raise HttpError(400, "the fixture does not replay segmented funnels")
    report = PROJECT["funnels"]["reports"].get(fid)
    if report is None:
        raise HttpError(400, f"funnel {fid} does not exist")
    data = {}
    for d, steps in sorted(report.get(unit, {}).items()):
        if f <= dt.date.fromisoformat(d) <= t:
            data[d] = {
                "steps": steps,
                "analysis": {"completion": steps[-1]["count"], "starting_amount": steps[0]["count"], "steps": len(steps), "worst": 1},
            }
    return {"meta": {"dates": list(data)}, "data": data}


@route("GET", "/api/query/retention", {"from_date", "to_date", "retention_type", "born_event", "event", "born_where", "where", "on", "limit", "interval", "interval_count", "unit", "unbounded_retention"})
def retention(h, q, form):
    f, t = date_range(q)
    rtype = choice(q, "retention_type", ("birth", "compounded"), "birth")
    unit = choice(q, "unit", ("day", "week", "month"), "day")
    count = int_param(q, "interval_count", 1, 1, 90)
    compile_expr(q.get("where"))
    compile_expr(q.get("born_where"))
    if rtype == "birth" and not q.get("born_event"):
        raise HttpError(400, "born_event is required for birth retention")
    rec = PROJECT["retention"].get(q.get("born_event") or "", {}).get(unit, {})
    out = {}
    for d, v in sorted(rec.items()):
        if f <= dt.date.fromisoformat(d) <= t:
            out[d] = {"counts": v["counts"][: (count or 1) + 1], "first": v["first"]}
    return out


@route("GET", "/api/query/events/top", {"type", "limit"})
def events_top(h, q, form):
    with LOCK:
        first = "events_top" not in ONCE
        ONCE.add("events_top")
    if first:
        raise HttpError(429, "rate limit exceeded: 60 queries per hour", {"Retry-After": "1"})
    typ = choice(q, "type", ("general", "unique", "average"), "general")
    limit = int_param(q, "limit", 100, 1, 1000)
    return {"events": PROJECT["top_events"][:limit], "type": typ}


@route("GET", "/api/query/events/names", {"type", "limit"})
def events_names(h, q, form):
    typ = choice(q, "type", ("general", "unique", "average"), "general")
    if typ == "average":
        raise HttpError(429, "Query rate limit exceeded", {"Retry-After": "0"})
    limit = int_param(q, "limit", 255, 1, 10_000)
    counts = Counter(e["event"] for e in all_events())
    return [n for n, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))][:limit]


@route("GET", "/api/query/events/properties/top", {"event", "limit"})
def properties_top(h, q, form):
    if not q.get("event"):
        raise HttpError(400, "event is a required parameter")
    limit = int_param(q, "limit", 10, 1, 1000)
    counts = Counter(k for e in all_events() if e["event"] == q["event"] for k in e["properties"])
    top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
    return {k: {"count": n} for k, n in top}


@route("GET", "/api/query/events/properties/values", {"event", "name", "values", "type", "unit", "limit"})
def properties_values(h, q, form):
    if not q.get("event") or not q.get("name"):
        raise HttpError(400, "event and name are required parameters")
    limit = int_param(q, "limit", 255, 1, 10_000)
    counts = Counter(
        str(e["properties"][q["name"]]) for e in all_events() if e["event"] == q["event"] and q["name"] in e["properties"]
    )
    return [v for v, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))][:limit]


ENGAGE_FORM = {"distinct_id", "distinct_ids", "data_group_id", "where", "output_properties", "session_id", "page", "behaviors", "as_of_timestamp", "filter_by_cohort", "include_all_users"}


@route("POST", "/api/query/engage", set())
def engage(h, q, form):
    unknown = set(form) - ENGAGE_FORM
    if unknown:
        raise HttpError(400, f"unknown parameter: {sorted(unknown)[0]}")
    page = int_param(form, "page", 0, 0, 100_000) or 0
    key = json.dumps({k: v for k, v in sorted(form.items()) if k not in ("page", "session_id")})
    if page > 0:
        sid = form.get("session_id")
        if not sid or SESSIONS.get(sid) != key:
            raise HttpError(400, "request for page in uncached query: pass the session_id from page 0")
    profiles = list(PROJECT["profiles"])
    if "filter_by_cohort" in form:
        try:
            cid = json.loads(form["filter_by_cohort"])["id"]
        except (ValueError, KeyError, TypeError) as err:
            raise HttpError(400, 'filter_by_cohort: expected {"id": <cohort id>}') from err
        cohort = next((c for c in PROJECT["cohorts"] if c["id"] == cid), None)
        if cohort is None:
            raise HttpError(400, f"cohort {cid} does not exist")
        by_id = {str(p["$distinct_id"]): p for p in profiles}
        profiles = [by_id.get(str(m), {"$distinct_id": m, "$properties": {}}) for m in cohort["members"]]
    if "distinct_id" in form:
        profiles = [p for p in profiles if str(p["$distinct_id"]) == form["distinct_id"]]
    if "distinct_ids" in form:
        try:
            ids = {str(x) for x in json.loads(form["distinct_ids"])}
        except (ValueError, TypeError) as err:
            raise HttpError(400, "distinct_ids: must be a JSON array") from err
        profiles = [p for p in profiles if str(p["$distinct_id"]) in ids]
    where = compile_expr(form.get("where"))
    profiles = [p for p in profiles if matches(where, p["$properties"])]
    size = PROJECT["engage_page_size"]
    chunk = profiles[page * size : (page + 1) * size]
    sid = f"fixture-{abs(hash(key)) % 10**8:08d}"
    with LOCK:
        SESSIONS[sid] = key
    body = {"page": page, "page_size": size, "results": chunk, "session_id": sid, "status": "ok"}
    if page == 0:
        body["total"] = len(profiles)
    return body


@route("POST", "/api/query/cohorts/list", set())
def cohorts_list(h, q, form):
    if form:
        raise HttpError(400, f"unknown parameter: {sorted(form)[0]}")
    return [{k: v for k, v in c.items() if k != "members"} for c in PROJECT["cohorts"]]


def app_schemas(h, q, form, entity: str | None):
    if q:
        raise HttpError(400, f"unknown parameter: {sorted(q)[0]}")
    if entity is not None and entity not in ("event", "profile"):
        raise HttpError(400, f"invalid entityType: {entity}")
    results = [s for s in PROJECT["schemas"] if entity is None or s["entityType"] == entity]
    return {"status": "ok", "results": results}


def app_annotations(h, q, form):
    unknown = set(q) - {"fromDate", "toDate"}
    if unknown:
        raise HttpError(400, f"unknown parameter: {sorted(unknown)[0]}")
    f = parse_date(q.get("fromDate"), "fromDate", False)
    t = parse_date(q.get("toDate"), "toDate", False)
    out = []
    for a in PROJECT["annotations"]:
        d = dt.date.fromisoformat(a["date"][:10])
        if (f is None or d >= f) and (t is None or d <= t):
            out.append(a)
    return {"status": "ok", "results": out}


# ------------------------------------------------------------------ server


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet
        pass

    def send_json(self, status: int, body, headers: dict | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")

    def dispatch(self, method: str) -> None:
        url = urlparse(self.path)
        raw_q = parse_qs(url.query, keep_blank_values=True)
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length).decode() if length else ""
        raw_f = parse_qs(raw_body, keep_blank_values=True) if raw_body else {}
        q = {k: v[-1] for k, v in raw_q.items()}
        form = {k: v[-1] for k, v in raw_f.items()}
        if url.path == "/__requests":
            want = q.get("path")
            with LOCK:
                rows = [r for r in REQUESTS if want is None or r["path"] == want]
            self.send_json(200, rows)
            return
        try:
            user = VALID_AUTH.get(self.headers.get("Authorization", ""))
            with LOCK:
                REQUESTS.append({
                    "seq": len(REQUESTS) + 1,
                    "method": method,
                    "path": url.path,
                    "query": json.dumps(q, sort_keys=True),
                    "form": json.dumps(form, sort_keys=True),
                    "user": user or "",
                })
            if user is None:
                raise HttpError(401, "Invalid service account credentials")
            m = re.fullmatch(r"/api/app/projects/([^/]+)/(schemas(?:/([^/]+))?|annotations)", url.path)
            if m:
                if method != "GET":
                    raise HttpError(405, "method not allowed")
                if m.group(1) != PROJECT_ID:
                    raise HttpError(403, f"you do not have access to project {m.group(1)}")
                body = app_annotations(self, q, form) if m.group(2) == "annotations" else app_schemas(self, q, form, m.group(3))
                self.send_json(200, body)
                return
            entry = ROUTES.get((method, url.path))
            if entry is None:
                if any(p == url.path for (_, p) in ROUTES):
                    raise HttpError(405, "method not allowed")
                raise HttpError(404, f"no such endpoint: {url.path}")
            allowed, fn = entry
            unknown = set(q) - allowed
            if unknown:
                raise HttpError(400, f"unknown parameter: {sorted(unknown)[0]}")
            if q.get("project_id") != PROJECT_ID:
                raise HttpError(403 if q.get("project_id") else 400, f"project_id {q.get('project_id')!r} is not accessible to this account")
            body = fn(self, q, form)
            if body is not None:
                self.send_json(200, body)
        except HttpError as err:
            self.send_json(err.status, {"error": str(err), "request": url.path, "status": "error"}, err.headers)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=0)
    args = ap.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    print(f"PORT:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
