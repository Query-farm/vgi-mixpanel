/**
 * Discovery metadata: the one file to edit when you rename or extend the worker.
 *
 * DuckDB, coding agents and the `vgi-lint` metadata checker all read these
 * tags. Keep every object documented and `make lint-explain` stays at 100.
 */

import { field, type VgiDataType, type VgiField } from "@query-farm/vgi";

export const CATALOG_NAME = "mixpanel";
export const VERSION = "0.1.0";

export const SOURCE_URL = "https://github.com/Query-farm/vgi-mixpanel";

/** Categories group functions in docs and agent tools; every object's `vgi.category` names one. */
export const SCHEMA_CATEGORIES = [
  { name: "events", title: "Raw Events", description: "Every tracked event from the Raw Event Export API." },
  {
    name: "reports",
    title: "Reports",
    description: "Query API reports: segmentation, funnels, retention and today's top events and properties.",
  },
  { name: "users", title: "Users and Cohorts", description: "User profiles (Engage API) and saved cohorts." },
  {
    name: "lexicon",
    title: "Lexicon and Annotations",
    description: "The data dictionary (Lexicon schemas for events and profile properties) and chart annotations.",
  },
  {
    name: "unified",
    title: "Shared Product-Analytics Shape",
    description: "Events, users, cohorts and definitions in the columns shared with the Amplitude and PostHog workers.",
  },
] as const;

export type Category = (typeof SCHEMA_CATEGORIES)[number]["name"];

/** Turn `"a, b, c"` into the JSON array string `vgi.keywords` requires. */
export function keywordsArray(keywords: string): string {
  return JSON.stringify(
    keywords
      .split(",")
      .map((k) => k.trim())
      .filter(Boolean),
  );
}

/** Build the standard per-object discovery tags. */
export function objectTags(opts: {
  title: string;
  category: Category;
  keywords: string;
  docLlm: string;
  docMd: string;
}): Record<string, string> {
  return {
    "vgi.title": opts.title,
    "vgi.category": opts.category,
    "vgi.keywords": keywordsArray(opts.keywords),
    "vgi.doc_llm": opts.docLlm,
    "vgi.doc_md": opts.docMd,
  };
}

/** Build an Arrow field whose `comment` shows up in DuckDB's `DESCRIBE` and agent tools. */
export function column(name: string, type: VgiDataType, comment: string, nullable = true): VgiField {
  return field(name, type, nullable, new Map([["comment", comment]]));
}

const SOURCES_MD =
  "### Sources and references\n\n" +
  "- [Raw Event Export API](https://docs.mixpanel.com/reference/raw-event-export)\n" +
  "- [Query API](https://docs.mixpanel.com/reference/query-api): segmentation, funnels, retention, engage, cohorts\n" +
  "- [Segmentation expressions](https://docs.mixpanel.com/reference/segmentation-expressions) (the `where` syntax)\n" +
  "- [Lexicon Schemas API](https://docs.mixpanel.com/reference/lexicon-schemas-api) and [Annotations](https://docs.mixpanel.com/reference/annotations)\n" +
  "- [Service accounts](https://docs.mixpanel.com/docs/orgs-and-projects/service-accounts) and [rate limits](https://docs.mixpanel.com/reference/rate-limits)";

export const CATALOG_COMMENT =
  "Mixpanel product analytics as SQL: raw events, segmentation, funnels, retention, user profiles, cohorts and the Lexicon, read-only.";

export const CATALOG_DOC_LLM =
  "Read-only SQL access to a Mixpanel project. Raw events stream from the Raw Event Export API " +
  "(mixpanel.main.events, or mixpanel.unified.events in the shared product-analytics shape); WHERE " +
  "event_time ranges choose the export days, event_name = / IN becomes the export event filter and string " +
  "= / IN on property columns (distinct_id, os, city, country_code, app_version …) becomes a Mixpanel " +
  "`where` expression, all re-checked exactly. Query API reports return long rows: segmentation (event " +
  "counts per period, optionally segmented by a property), funnel (per period and step), retention (cohort " +
  "x period), plus funnels (saved funnel ids), top_events, event_names, top_event_properties and " +
  "event_property_values. profiles reads user profiles through the Engage API (where_expr, cohort_id, " +
  "distinct_id pushdown); cohorts, schemas, schema_properties and annotations are tables. The unified " +
  "schema (events, users, cohorts, event_definitions, property_definitions) uses the same column names as " +
  "the Amplitude and PostHog workers so rows UNION ALL across tools. Every object needs a `mixpanel` " +
  "secret: service account USERNAME + SECRET (or a project API_SECRET), PROJECT_ID and REGION (us, eu, " +
  "in). Mixpanel allows 60 Query API calls per hour (5 concurrent) and 60 exports per hour, so filter " +
  "early. JQL is deprecated and not exposed. Docs: https://docs.mixpanel.com/reference/overview .";

export const CATALOG_DOC_MD =
  "# Mixpanel\n\n" +
  "Query a Mixpanel project from DuckDB: raw events, segmentation, funnels and retention reports, user " +
  "profiles, cohorts, the Lexicon data dictionary and annotations. Read-only.\n\n" +
  "Create a `mixpanel` secret with a service account (`USERNAME`, `SECRET`), the numeric `PROJECT_ID` " +
  "and the data residency `REGION` (`us`, `eu` or `in`). Filters on event time, event name and common " +
  "properties are sent to Mixpanel, so only matching events cross the network.\n\n" +
  "The `unified` schema shares its column names with the Amplitude and PostHog workers.\n\n" +
  SOURCES_MD;

export const SCHEMA_DOC_LLM =
  "Mixpanel-native objects. events(from_date :=, to_date :=, where_expr :=, lookback_days :=, chunk_days :=) " +
  "streams raw events (default: the last 7 days). segmentation(event, from_date, to_date, ...), " +
  "funnel(funnel_id, from_date, to_date, ...) and retention(from_date, to_date, ...) run Query API reports " +
  "and return long rows. profiles(...) reads Engage user profiles. cohorts, funnels, schemas, " +
  "schema_properties and annotations are tables. All need a `mixpanel` secret.";

export const SCHEMA_DOC_MD =
  "## main\n\n" +
  "Mixpanel-native objects: raw events from the export API, Query API reports (segmentation, funnels, " +
  "retention, top events and properties), user profiles and cohorts, Lexicon schemas and annotations.\n\n" +
  "All objects need a `mixpanel` secret.";

export const UNIFIED_DOC_LLM =
  "Mixpanel data in the shared product-analytics shape (the same columns in the Amplitude and PostHog " +
  "workers): events (tool, event_id, event_name, event_time, received_time, user_id, device_id, distinct_id, " +
  "session_id, insert_id, properties, user_properties, platform, os, country, city, app_version, library, " +
  "revenue), users, cohorts, event_definitions and property_definitions. Use them to UNION ALL across tools; " +
  "use the main schema for every Mixpanel field.";

export const UNIFIED_DOC_MD =
  "## unified\n\n" +
  "Events, users, cohorts and data-dictionary definitions with the column names shared by the Query.Farm " +
  "product-analytics workers (Amplitude, Mixpanel, PostHog), so the same query runs against each.\n\n" +
  "Columns a tool cannot fill are NULL rather than missing: Mixpanel has no event uuid (the `$insert_id` " +
  "is used), no user properties on raw events and no cohort definitions in its cohort list.\n\n" +
  "Use the `main` schema for every Mixpanel field, the reports and the cohort member lists.";

export const AGENT_TEST_TASKS = [
  { name: "signups_by_os", prompt: "How many 'Sign Up' events were there per operating system on 2026-11-02?" },
  {
    name: "purchase_revenue",
    prompt: "What was the total revenue from Purchase events between 2026-11-01 and 2026-11-03?",
  },
  { name: "ios_users", prompt: "How many distinct users sent events from iOS during 2026-11-01 through 2026-11-03?" },
  {
    name: "daily_signups",
    prompt: "Using a segmentation report, how many Sign Up events happened each day from 2026-11-01 to 2026-11-03?",
  },
  {
    name: "signup_funnel",
    prompt: "What is the overall conversion of the last step of the 'Signup funnel' for the week of 2026-11-02?",
  },
  { name: "retention_day1", prompt: "What share of users who signed up on 2026-11-01 came back one day later?" },
  { name: "top_event_today", prompt: "Which event is most common in Mixpanel today?" },
  { name: "pro_users", prompt: "List the email addresses of users on the pro plan." },
  { name: "biggest_cohort", prompt: "Which Mixpanel cohort has the most users?" },
  { name: "hidden_events", prompt: "Which events are hidden in the Lexicon?" },
  {
    name: "purchase_properties",
    prompt: "Which properties does the Lexicon define for the Purchase event, and of what type?",
  },
  { name: "november_annotations", prompt: "What annotations were added in November 2026?" },
  { name: "purchase_property_frequency", prompt: "Which properties are sent most often with Purchase events?" },
  {
    name: "purchase_plan_values",
    prompt: "Which values does the plan property take on Purchase events, most common first?",
  },
  { name: "common_event_names", prompt: "What are the three most common event names over the last month?" },
  {
    name: "shared_cohort_sizes",
    prompt: "Using the shared product-analytics tables, list every cohort with its size, largest first.",
  },
  {
    name: "shared_inactive_events",
    prompt: "Using the shared product-analytics tables, which events in the tracking plan are not active?",
  },
  {
    name: "shared_user_properties",
    prompt: "Using the shared product-analytics tables, which user-level properties are defined, and of what type?",
  },
  {
    name: "shared_users_by_country",
    prompt: "Using the shared product-analytics tables, how many users are there per country?",
  },
];

/** Bind-only probes: each binds the function and shows its columns without fetching (LIMIT 0). */
export const EXECUTABLE_EXAMPLES = [
  {
    name: "events_columns",
    description: "The events() result columns (binds only; attach a mixpanel secret and drop LIMIT 0 for rows)",
    sql: "SELECT event_name, event_time, distinct_id, os, properties FROM mixpanel.main.events() LIMIT 0",
  },
  {
    name: "segmentation_columns",
    description: "The segmentation() result columns (binds only; attach a mixpanel secret and drop LIMIT 0 for rows)",
    sql: "SELECT event, segment, period_start, value FROM mixpanel.main.segmentation('Sign Up', '2026-11-01', '2026-11-03') LIMIT 0",
  },
];

export const CATALOG_TAGS: Record<string, string> = {
  "vgi.title": "Mixpanel Product Analytics",
  "vgi.subtitle": "Mixpanel raw events, reports, user profiles, cohorts and Lexicon definitions as SQL tables.",
  "vgi.doc_llm": CATALOG_DOC_LLM,
  "vgi.doc_md": CATALOG_DOC_MD,
  "vgi.source_url": SOURCE_URL,
  "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
  "vgi.license": "0BSD",
  "vgi.author": "Query Farm LLC",
  "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
  "vgi.keywords": keywordsArray(
    "mixpanel, product analytics, events, funnels, retention, segmentation, cohorts, user profiles, lexicon, amplitude, posthog",
  ),
  "vgi.support_contact": `${SOURCE_URL}/issues`,
  "vgi.support_policy_url": `${SOURCE_URL}#support`,
  // Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
  "vgi.agent_test_tasks": JSON.stringify(AGENT_TEST_TASKS),
  "vgi.executable_examples": JSON.stringify(EXECUTABLE_EXAMPLES),
};

export const SCHEMA_TAGS: Record<string, string> = {
  "vgi.title": "Mixpanel APIs",
  domain: "product-analytics",
  "vgi.keywords": keywordsArray("mixpanel, events, export, segmentation, funnels, retention, engage, cohorts, lexicon"),
  "vgi.doc_llm": SCHEMA_DOC_LLM,
  "vgi.doc_md": SCHEMA_DOC_MD,
  "vgi.categories": JSON.stringify(SCHEMA_CATEGORIES.filter((c) => c.name !== "unified")),
};

export const UNIFIED_SCHEMA_TAGS: Record<string, string> = {
  "vgi.title": "Shared Product-Analytics Shape",
  domain: "product-analytics",
  "vgi.keywords": keywordsArray("product analytics, unified, events, users, cohorts, amplitude, posthog, mixpanel"),
  "vgi.doc_llm": UNIFIED_DOC_LLM,
  "vgi.doc_md": UNIFIED_DOC_MD,
  "vgi.categories": JSON.stringify(SCHEMA_CATEGORIES.filter((c) => c.name === "unified")),
};
