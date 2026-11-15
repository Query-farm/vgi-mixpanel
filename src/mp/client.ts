/**
 * Mixpanel API client: the `mixpanel` secret, region hosts, HTTP Basic auth,
 * rate limits and errors.
 *
 * - **Auth.** A service account (`USERNAME` + `SECRET`, HTTP Basic) plus the
 *   `PROJECT_ID` every request carries, or a legacy project `API_SECRET` (Basic with
 *   the secret as the user name). Credentials are read from the secret on every call
 *   and never enter table-function state.
 * - **Hosts.** Mixpanel serves three families from region-specific hosts:
 *   raw export (`data.mixpanel.com`, `data-eu`, `data-in`), the Query API
 *   (`mixpanel.com/api/query`, `eu.`, `in.`) and the App API (`mixpanel.com/api/app`
 *   for Lexicon schemas and annotations). `BASE_URL` replaces all three hosts with one
 *   origin (a proxy or the offline fixture server).
 * - **Rate limits.** The Query API allows 60 queries per hour and 5 concurrent
 *   queries; raw export allows 60 per hour, 3 per second and 100 concurrent. A
 *   per-process semaphore keeps at most 5 Query API calls in flight, export calls are
 *   spaced at least 350 ms apart, and HTTP 429 / 5xx retry with backoff (honouring
 *   `Retry-After`). A 429 that persists past the retry budget fails with a message
 *   naming the limit rather than hanging for the rest of the hour.
 */

import { secretOfType } from "@query-farm/vgi";
import { log } from "../log.ts";
import { VERSION } from "../meta.ts";

export const SECRET_TYPE = "mixpanel";
export const TIMEOUT_MS = 120_000;
export const MAX_ATTEMPTS = 5;
/** Longest single wait for one 429 / 5xx retry. */
export const MAX_RETRY_WAIT_MS = 60_000;
/** The Query API's documented concurrency limit. */
export const QUERY_CONCURRENCY = 5;
/** Raw export allows 3 requests per second. */
const EXPORT_SPACING_MS = 350;
const USER_AGENT = `vgi-mixpanel-worker/${VERSION}`;

export type Region = "us" | "eu" | "in";
export type Family = "export" | "query" | "app";

const HOSTS: Record<Region, Record<Family, string>> = {
  us: { export: "https://data.mixpanel.com", query: "https://mixpanel.com", app: "https://mixpanel.com" },
  eu: { export: "https://data-eu.mixpanel.com", query: "https://eu.mixpanel.com", app: "https://eu.mixpanel.com" },
  in: { export: "https://data-in.mixpanel.com", query: "https://in.mixpanel.com", app: "https://in.mixpanel.com" },
};

/** Mixpanel answered with an error (status set) or the request could not be made. */
export class MixpanelError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
  }
}

export interface MpConfig {
  projectId: string;
  region: Region;
  /** `Basic …` header value. */
  auth: string;
  baseUrl?: string;
  workspaceId?: string;
}

const LOOPBACK = new Set(["127.0.0.1", "localhost", "[::1]", "::1"]);

/** Normalize a BASE_URL override: https, or http only for a loopback fixture server. */
export function normalizeBaseUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new MixpanelError(`mixpanel secret: base_url '${raw}' is not a URL`);
  }
  if (url.protocol !== "https:" && !(url.protocol === "http:" && LOOPBACK.has(url.hostname))) {
    throw new MixpanelError("mixpanel secret: base_url must use https:// (http:// only for 127.0.0.1/localhost)");
  }
  return url.origin + url.pathname.replace(/\/+$/, "");
}

const str = (v: unknown): string | undefined =>
  typeof v === "string" && v.trim() !== ""
    ? v.trim()
    : typeof v === "number" || typeof v === "bigint"
      ? String(v)
      : undefined;

/** Build the client config from the `mixpanel` secret DuckDB resolved for this call. */
export function configFromSecrets(secrets: Record<string, Record<string, unknown>>): MpConfig {
  const s = secretOfType(secrets as never, SECRET_TYPE) as Record<string, unknown> | undefined;
  if (!s) {
    throw new MixpanelError(
      "no mixpanel secret: run CREATE SECRET (TYPE mixpanel, USERNAME '<service account>', SECRET '<sa secret>', " +
        "PROJECT_ID '<id>', REGION 'us')",
    );
  }
  const projectId = str(s.project_id);
  if (!projectId || !/^\d+$/.test(projectId)) {
    throw new MixpanelError("mixpanel secret: project_id is required and must be the numeric project id");
  }
  const region = (str(s.region) ?? "us").toLowerCase();
  if (region !== "us" && region !== "eu" && region !== "in") {
    throw new MixpanelError(`mixpanel secret: region must be 'us', 'eu' or 'in' (got '${region}')`);
  }
  const username = str(s.username);
  const secret = str(s.secret);
  const apiSecret = str(s.api_secret);
  let auth: string;
  if (username && secret) auth = `Basic ${Buffer.from(`${username}:${secret}`).toString("base64")}`;
  else if (apiSecret) auth = `Basic ${Buffer.from(`${apiSecret}:`).toString("base64")}`;
  else {
    throw new MixpanelError("mixpanel secret: set USERNAME and SECRET (service account), or a project API_SECRET");
  }
  const workspaceId = str(s.workspace_id);
  if (workspaceId && !/^\d+$/.test(workspaceId))
    throw new MixpanelError("mixpanel secret: workspace_id must be numeric");
  const base = str(s.base_url);
  return { projectId, region, auth, baseUrl: base ? normalizeBaseUrl(base) : undefined, workspaceId };
}

/** The origin a request family goes to. */
export function hostOf(cfg: MpConfig, family: Family): string {
  return cfg.baseUrl ?? HOSTS[cfg.region][family];
}

export type QueryValue = string | number | undefined | null;

export interface MpRequest {
  family: Family;
  method?: "GET" | "POST";
  /** Path below the host, e.g. `/api/query/segmentation`. `{project}` is replaced by the project id. */
  path: string;
  query?: Record<string, QueryValue>;
  /** POST form fields (`application/x-www-form-urlencoded`). */
  form?: Record<string, QueryValue>;
}

/** Build the request URL; `project_id` (and `workspace_id`) go on every Query / export call. */
export function urlOf(cfg: MpConfig, req: MpRequest): string {
  const qs = new URLSearchParams();
  if (req.family !== "app") {
    qs.set("project_id", cfg.projectId);
    if (cfg.workspaceId) qs.set("workspace_id", cfg.workspaceId);
  }
  for (const [k, v] of Object.entries(req.query ?? {}))
    if (v !== undefined && v !== null && v !== "") qs.set(k, String(v));
  const q = qs.toString();
  const path = req.path.replace("{project}", encodeURIComponent(cfg.projectId));
  return `${hostOf(cfg, req.family)}${path}${q ? `?${q}` : ""}`;
}

// ------------------------------------------------------------------ throttling

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A counting semaphore (the Query API's 5-concurrent limit, per worker process). */
class Semaphore {
  private waiters: (() => void)[] = [];
  constructor(private free: number) {}
  async acquire(): Promise<() => void> {
    if (this.free > 0) this.free--;
    else await new Promise<void>((r) => this.waiters.push(r));
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.waiters.shift();
      if (next) next();
      else this.free++;
    };
  }
}
const querySlots = new Semaphore(QUERY_CONCURRENCY);
let lastExportAt = 0;

/** Delay for retry `attempt` (1-based): Retry-After when given, else exponential. */
export function retryDelayMs(headers: Headers | null, attempt: number): number {
  const ra = headers?.get("Retry-After");
  if (ra && /^\d+(\.\d+)?$/.test(ra.trim())) return Math.min(Number(ra) * 1000, MAX_RETRY_WAIT_MS);
  if (ra) {
    const at = Date.parse(ra);
    if (!Number.isNaN(at)) return Math.min(Math.max(0, at - Date.now()), MAX_RETRY_WAIT_MS);
  }
  return Math.min(1000 * 2 ** (attempt - 1), MAX_RETRY_WAIT_MS);
}

/** Mixpanel error bodies: `{"error": "...", "request": "..."}` or `{"status":"error","error":"..."}`. */
export function errorDetail(text: string): string {
  try {
    const j = JSON.parse(text) as { error?: unknown; message?: unknown };
    const e = j.error ?? j.message;
    if (typeof e === "string" && e) return e;
  } catch {}
  return text.trim().slice(0, 300) || "(empty body)";
}

/** Path + query of a URL for messages (credentials are never in URLs). */
function shown(url: string): string {
  const u = new URL(url);
  return `${u.pathname}${u.search}`;
}

/**
 * Send one request and return the successful Response (body unread), retrying 429,
 * 5xx and network errors. The caller must consume or cancel the body; Query API calls
 * hold a concurrency slot until `release` is called.
 */
export async function mpFetch(cfg: MpConfig, req: MpRequest): Promise<{ res: Response; release: () => void }> {
  const url = urlOf(cfg, req);
  const method = req.method ?? "GET";
  let body: string | undefined;
  const headers: Record<string, string> = {
    Accept: req.family === "export" ? "text/plain, application/json" : "application/json",
    Authorization: cfg.auth,
    "User-Agent": USER_AGENT,
  };
  if (req.form) {
    const f = new URLSearchParams();
    for (const [k, v] of Object.entries(req.form)) if (v !== undefined && v !== null && v !== "") f.set(k, String(v));
    body = f.toString();
    headers["Content-Type"] = "application/x-www-form-urlencoded";
  }
  for (let attempt = 1; ; attempt++) {
    const release = req.family === "query" ? await querySlots.acquire() : () => {};
    if (req.family === "export") {
      const wait = lastExportAt + EXPORT_SPACING_MS - Date.now();
      if (wait > 0) await sleep(wait);
      lastExportAt = Date.now();
    }
    let res: Response;
    try {
      res = await fetch(url, { method, headers, body, signal: AbortSignal.timeout(TIMEOUT_MS) });
    } catch (err) {
      release();
      if (attempt >= MAX_ATTEMPTS) {
        throw new MixpanelError(`${method} ${shown(url)} failed: ${(err as Error).message}`);
      }
      await sleep(retryDelayMs(null, attempt));
      continue;
    }
    if (res.ok) return { res, release };
    const text = await res.text().catch(() => "");
    release();
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt >= MAX_ATTEMPTS) {
      if (res.status === 429) {
        throw new MixpanelError(
          `Mixpanel rate limit exceeded (HTTP 429) for ${shown(url)} after ${attempt} attempts: ${errorDetail(text)}. ` +
            "The Query API allows 60 queries per hour and 5 concurrent queries; raw export allows 60 per hour " +
            "and 3 per second. Wait and re-run the query.",
          429,
        );
      }
      const hint =
        res.status === 401 || res.status === 403
          ? " (check the mixpanel secret: service account USERNAME/SECRET, its project role, PROJECT_ID and REGION)"
          : "";
      throw new MixpanelError(
        `${method} ${shown(url)} failed with HTTP ${res.status}: ${errorDetail(text)}${hint}`,
        res.status,
      );
    }
    const delay = retryDelayMs(res.headers, attempt);
    log.info(`mixpanel: HTTP ${res.status} for ${shown(url)}; retry ${attempt} in ${delay} ms`);
    await sleep(delay);
  }
}

/** Send a request and decode its JSON body. */
export async function mpJson(cfg: MpConfig, req: MpRequest): Promise<unknown> {
  const { res, release } = await mpFetch(cfg, req);
  try {
    const text = await res.text();
    if (!text.trim()) return null;
    try {
      return JSON.parse(text);
    } catch {
      throw new MixpanelError(`${req.path}: Mixpanel returned a non-JSON body: ${text.slice(0, 200)}`);
    }
  } finally {
    release();
  }
}
