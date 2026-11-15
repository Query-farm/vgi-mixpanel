/**
 * Assemble the `mixpanel` catalog: the `main` schema (Mixpanel-native functions and
 * tables), the `unified` schema (the shared product-analytics shape) and the
 * `mixpanel` secret type.
 */

import { type CatalogDescriptor, field, schema, type TableDescriptor, utf8, type VgiFunction } from "@query-farm/vgi";
import {
  CATALOG_COMMENT,
  CATALOG_NAME,
  CATALOG_TAGS,
  SCHEMA_DOC_MD,
  SCHEMA_TAGS,
  SOURCE_URL,
  UNIFIED_DOC_MD,
  UNIFIED_SCHEMA_TAGS,
} from "./meta.ts";
import { SECRET_TYPE } from "./mp/client.ts";
import { MAIN_FUNCTIONS, MAIN_TABLES, UNIFIED_TABLES } from "./mp/resources.ts";

export const FUNCTIONS = MAIN_FUNCTIONS;

const doc = (text: string) => new Map([["vgi_doc", text]]);
const redacted = (text: string) =>
  new Map([
    ["redact", "true"],
    ["vgi_doc", text],
  ]);

/** CREATE SECRET (TYPE mixpanel, USERNAME '…', SECRET '…', PROJECT_ID '…', REGION 'us'). */
export const MIXPANEL_SECRET_TYPE = {
  name: SECRET_TYPE,
  description: "Mixpanel service account (or project API secret), project id and data residency region",
  schema: schema(
    [
      field("username", utf8(), true, doc("Service account username (Organization Settings > Service Accounts).")),
      field("secret", utf8(), true, redacted("Service account secret.")),
      field("project_id", utf8(), true, doc("Numeric Mixpanel project id (Project Settings > Overview).")),
      field("region", utf8(), true, doc("Data residency: 'us' (default), 'eu' or 'in'.")),
      field(
        "api_secret",
        utf8(),
        true,
        redacted("Legacy project API secret, instead of a service account (still needs PROJECT_ID)."),
      ),
      field("workspace_id", utf8(), true, doc("Optional workspace (data view) id for Query API calls.")),
      field(
        "base_url",
        utf8(),
        true,
        doc("Optional origin that replaces every Mixpanel host (a proxy or a local fixture server)."),
      ),
    ],
    new Map([["vgi_doc_url", "https://docs.mixpanel.com/docs/orgs-and-projects/service-accounts"]]),
  ),
};

interface Example {
  description: string;
  sql: string;
}

/** Publish each function's examples as `vgi.example_queries`; return them all. */
function stampExampleQueries(functions: VgiFunction[]): Example[] {
  const all: Example[] = [];
  for (const fn of functions) {
    const ex = (fn.meta.examples ?? []).map((e) => ({ description: e.description, sql: e.sql }));
    fn.meta.tags = { ...fn.meta.tags, "vgi.example_queries": JSON.stringify(ex) };
    all.push(...ex);
  }
  return all;
}

const tableExamples = (tables: TableDescriptor[]) =>
  tables.flatMap((t) => JSON.parse(t.tags?.["vgi.example_queries"] ?? "[]") as Example[]);

const MAIN_EXAMPLES = [...stampExampleQueries(FUNCTIONS), ...tableExamples(MAIN_TABLES)];

export const CATALOG: CatalogDescriptor = {
  name: CATALOG_NAME,
  comment: CATALOG_COMMENT,
  sourceUrl: SOURCE_URL,
  tags: CATALOG_TAGS,
  secretTypes: [MIXPANEL_SECRET_TYPE],
  schemas: [
    {
      path: ["main"],
      comment: SCHEMA_DOC_MD,
      functions: FUNCTIONS,
      tables: MAIN_TABLES,
      tags: { ...SCHEMA_TAGS, "vgi.example_queries": JSON.stringify(MAIN_EXAMPLES.slice(0, 12)) },
    },
    {
      path: ["unified"],
      comment: UNIFIED_DOC_MD,
      functions: [],
      tables: UNIFIED_TABLES,
      tags: { ...UNIFIED_SCHEMA_TAGS, "vgi.example_queries": JSON.stringify(tableExamples(UNIFIED_TABLES)) },
    },
  ],
};
