#!/usr/bin/env node
/**
 * Bulk-update Contract End Date on C&I Electricity Records from a CSV.
 *
 * Usage:
 *   node scripts/bulk_update_contract_end_date.mjs [--csv PATH] [--dry-run|--apply]
 *
 * Defaults to --dry-run. Loads AIRTABLE_API_KEY / AIRTABLE_BASE_ID from
 * text_agent_backend/.env (or the process environment).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BACKEND_ROOT = path.resolve(__dirname, "..");
const DEFAULT_CSV = path.join(
  process.env.USERPROFILE || process.env.HOME || "",
  "Downloads",
  "airtable_CED_update.csv",
);

const TABLE_NAME = "C&I Electricity Records";
const FIELD_NMI = "NMI";
const FIELD_CED = "Contract End Date";
const BATCH_SIZE = 10;
const MIN_REQUEST_INTERVAL_MS = 210; // Airtable: 5 req/sec

// ---------------------------------------------------------------------------
// Env / CLI
// ---------------------------------------------------------------------------

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const text = fs.readFileSync(filePath, "utf8");
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    let val = line.slice(eq + 1).trim();
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (process.env[key] === undefined) process.env[key] = val;
  }
}

function parseArgs(argv) {
  const opts = {
    csv: DEFAULT_CSV,
    apply: false,
    dryRun: true,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply") {
      opts.apply = true;
      opts.dryRun = false;
    } else if (a === "--dry-run") {
      opts.apply = false;
      opts.dryRun = true;
    } else if (a === "--csv") {
      opts.csv = argv[++i];
    } else if (a.startsWith("--csv=")) {
      opts.csv = a.slice("--csv=".length);
    } else if (a === "--help" || a === "-h") {
      opts.help = true;
    } else {
      console.error(`Unknown argument: ${a}`);
      process.exit(2);
    }
  }
  return opts;
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
      continue;
    }
    if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      row.push(field);
      field = "";
    } else if (c === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (c === "\r") {
      // ignore; handle \r\n via \n
    } else {
      field += c;
    }
  }
  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }
  if (!rows.length) return [];
  const headers = rows[0].map((h) => h.trim());
  return rows.slice(1).filter((r) => r.some((c) => c.trim() !== "")).map((r) => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = (r[i] ?? "").trim();
    }
    return obj;
  });
}

function escapeCsv(val) {
  const s = val == null ? "" : String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => escapeCsv(row[h])).join(","));
  }
  fs.writeFileSync(filePath, lines.join("\n") + "\n", "utf8");
}

// ---------------------------------------------------------------------------
// NMI matching
// ---------------------------------------------------------------------------

function normalizeNmi(raw) {
  return String(raw ?? "").trim();
}

/** Exact string match only (CSV was exported from Airtable). */
function nmisMatch(a, b) {
  if (!a || !b) return null;
  return a === b ? "exact" : null;
}

function normalizeDate(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return "";
  // Airtable returns YYYY-MM-DD; accept that or trailing time
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : s;
}

function datesEqual(a, b) {
  return normalizeDate(a) === normalizeDate(b);
}

// ---------------------------------------------------------------------------
// Airtable client
// ---------------------------------------------------------------------------

class AirtableClient {
  constructor(apiKey, baseId) {
    this.apiKey = apiKey;
    this.baseId = baseId;
    this.lastRequestAt = 0;
  }

  async throttle() {
    const elapsed = Date.now() - this.lastRequestAt;
    if (elapsed < MIN_REQUEST_INTERVAL_MS) {
      await new Promise((r) => setTimeout(r, MIN_REQUEST_INTERVAL_MS - elapsed));
    }
    this.lastRequestAt = Date.now();
  }

  async request(method, urlPath, body) {
    await this.throttle();
    const url = `https://api.airtable.com/v0/${this.baseId}/${urlPath}`;
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json;
    try {
      json = text ? JSON.parse(text) : {};
    } catch {
      json = { raw: text };
    }
    if (!res.ok) {
      const err = new Error(
        `Airtable ${method} ${urlPath} -> ${res.status}: ${text.slice(0, 500)}`,
      );
      err.status = res.status;
      err.body = json;
      throw err;
    }
    return json;
  }

  tablePath(extra = "") {
    return encodeURIComponent(TABLE_NAME) + extra;
  }

  async fetchOneSample() {
    return this.request(
      "GET",
      this.tablePath(`?maxRecords=1&fields[]=${encodeURIComponent(FIELD_NMI)}&fields[]=${encodeURIComponent(FIELD_CED)}`),
    );
  }

  async listAllRecords() {
    const records = [];
    let offset;
    do {
      const qs = new URLSearchParams();
      qs.append("pageSize", "100");
      qs.append("fields[]", FIELD_NMI);
      qs.append("fields[]", FIELD_CED);
      if (offset) qs.append("offset", offset);
      const data = await this.request("GET", this.tablePath(`?${qs}`));
      records.push(...(data.records || []));
      offset = data.offset;
    } while (offset);
    return records;
  }

  async updateBatch(updates) {
    // updates: [{ id, fields: { "Contract End Date": "YYYY-MM-DD" } }]
    return this.request("PATCH", this.tablePath(), { records: updates });
  }
}

// ---------------------------------------------------------------------------
// Matching / planning
// ---------------------------------------------------------------------------

function findMatches(csvNmi, airtableIndex) {
  const hits = [];
  for (const rec of airtableIndex) {
    const mt = nmisMatch(csvNmi, rec.nmi);
    if (mt) hits.push({ ...rec, matchType: mt });
  }
  return hits;
}

function planRow(csvRow, airtableIndex) {
  const nmi = normalizeNmi(csvRow.NMI);
  const expectedCurrent = normalizeDate(csvRow.Current_Airtable_End_Date);
  const newDate = normalizeDate(csvRow.New_End_Date);

  if (!nmi) {
    return {
      nmi: "",
      recordId: "",
      oldValue: "",
      newValue: newDate,
      matchType: "",
      status: "error",
      detail: "empty NMI",
    };
  }
  if (!newDate) {
    return {
      nmi,
      recordId: "",
      oldValue: "",
      newValue: "",
      matchType: "",
      status: "error",
      detail: "empty New_End_Date",
    };
  }

  const hits = findMatches(nmi, airtableIndex);
  if (hits.length === 0) {
    return {
      nmi,
      recordId: "",
      oldValue: expectedCurrent,
      newValue: newDate,
      matchType: "",
      status: "not-found",
      detail: "",
    };
  }
  if (hits.length > 1) {
    return {
      nmi,
      recordId: hits.map((h) => h.id).join(";"),
      oldValue: hits.map((h) => h.ced || "").join(";"),
      newValue: newDate,
      matchType: hits.map((h) => h.matchType).join(";"),
      status: "skipped-ambiguous",
      detail: `${hits.length} records`,
    };
  }

  const hit = hits[0];
  if (!datesEqual(hit.ced, expectedCurrent)) {
    return {
      nmi,
      recordId: hit.id,
      oldValue: hit.ced,
      newValue: newDate,
      matchType: hit.matchType,
      status: "skipped-drift",
      detail: `expected ${expectedCurrent || "(blank)"} got ${hit.ced || "(blank)"}`,
    };
  }

  return {
    nmi,
    recordId: hit.id,
    oldValue: hit.ced,
    newValue: newDate,
    matchType: hit.matchType,
    status: "ready",
    detail: "",
  };
}

function pad(s, w) {
  const str = s == null ? "" : String(s);
  return str.length >= w ? str : str + " ".repeat(w - str.length);
}

function printPlanTable(plans) {
  const cols = [
    ["NMI", 14],
    ["Airtable record id", 20],
    ["current value", 14],
    ["new value", 12],
    ["match type", 18],
    ["status", 20],
  ];
  console.log(cols.map(([h, w]) => pad(h, w)).join(" | "));
  console.log(cols.map(([, w]) => "-".repeat(w)).join("-+-"));
  for (const p of plans) {
    console.log(
      [
        pad(p.nmi, 14),
        pad(p.recordId, 20),
        pad(p.oldValue || "(blank)", 14),
        pad(p.newValue || "(blank)", 12),
        pad(p.matchType || "-", 18),
        pad(p.status, 20),
      ].join(" | "),
    );
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  loadDotEnv(path.join(BACKEND_ROOT, ".env"));
  const opts = parseArgs(process.argv.slice(2));

  if (opts.help) {
    console.log(`Usage: node scripts/bulk_update_contract_end_date.mjs [--csv PATH] [--dry-run|--apply]
Default CSV: ${DEFAULT_CSV}
Default mode: --dry-run`);
    process.exit(0);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || "appG1WoHcJt10iO5K";
  if (!apiKey) {
    console.error("Missing AIRTABLE_API_KEY (set in env or text_agent_backend/.env)");
    process.exit(1);
  }
  if (!fs.existsSync(opts.csv)) {
    console.error(`CSV not found: ${opts.csv}`);
    process.exit(1);
  }

  const client = new AirtableClient(apiKey, baseId);
  console.log(`Mode: ${opts.apply ? "APPLY (will write)" : "DRY-RUN (no writes)"}`);
  console.log(`Base: ${baseId}`);
  console.log(`Table: ${TABLE_NAME}`);
  console.log(`CSV:   ${opts.csv}`);
  console.log("");

  // Confirm field name + type via sample record
  console.log("--- Field verification (sample record) ---");
  const sample = await client.fetchOneSample();
  const sampleRec = sample.records?.[0];
  if (!sampleRec) {
    console.error("No records in table — aborting.");
    process.exit(1);
  }
  const sampleFields = sampleRec.fields || {};
  console.log(`Sample record id: ${sampleRec.id}`);
  console.log(`Fields present on sample:`);
  for (const [k, v] of Object.entries(sampleFields)) {
    console.log(`  ${k}: ${JSON.stringify(v)} (${typeof v})`);
  }
  if (!(FIELD_CED in sampleFields) && !(FIELD_NMI in sampleFields)) {
    // Sample might have blank CED — still confirm via keys we requested
    console.log(
      `Requested fields: ${FIELD_NMI} (text), ${FIELD_CED} (date). Sample may omit blank dates.`,
    );
  }
  console.log(
    `Confirmed target field name: "${FIELD_CED}" (Airtable type: date — see meta earlier / value shape YYYY-MM-DD)`,
  );
  console.log(`Confirmed match field name: "${FIELD_NMI}"`);
  console.log("");

  // Load CSV
  const csvRows = parseCsv(fs.readFileSync(opts.csv, "utf8"));
  console.log(`CSV rows: ${csvRows.length}`);

  // Index Airtable
  console.log("Fetching all C&I Electricity Records (NMI + Contract End Date)...");
  const all = await client.listAllRecords();
  const airtableIndex = all.map((r) => ({
    id: r.id,
    nmi: normalizeNmi(r.fields?.[FIELD_NMI]),
    ced: normalizeDate(r.fields?.[FIELD_CED]),
  }));
  console.log(`Airtable records loaded: ${airtableIndex.length}`);
  console.log("");

  const plans = csvRows.map((row) => planRow(row, airtableIndex));
  printPlanTable(plans);
  console.log("");

  const counts = {
    matched: 0,
    ready: 0,
    updated: 0,
    "skipped-ambiguous": 0,
    "skipped-drift": 0,
    "not-found": 0,
    error: 0,
  };
  for (const p of plans) {
    if (p.recordId && p.status !== "not-found") counts.matched++;
    if (p.status === "ready") counts.ready++;
    else if (counts[p.status] !== undefined) counts[p.status]++;
  }

  const logRows = [];
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const logPath = path.join(
    path.dirname(opts.csv),
    `airtable_CED_update_runlog_${stamp}.csv`,
  );

  if (!opts.apply) {
    for (const p of plans) {
      const status = p.status === "ready" ? "ready (dry-run)" : p.status;
      logRows.push({
        NMI: p.nmi,
        "record id": p.recordId,
        "old value": p.oldValue,
        "new value": p.newValue,
        status,
        detail: p.detail,
        "match type": p.matchType,
      });
    }
    writeCsv(
      logPath,
      ["NMI", "record id", "old value", "new value", "status", "detail", "match type"],
      logRows,
    );
    console.log("--- Summary (dry-run) ---");
    console.log(`matched (unique CSV→record): ${counts.matched}`);
    console.log(`would update:                ${counts.ready}`);
    console.log(`skipped-ambiguous:           ${counts["skipped-ambiguous"]}`);
    console.log(`skipped-drift:               ${counts["skipped-drift"]}`);
    console.log(`not-found:                   ${counts["not-found"]}`);
    console.log(`error:                       ${counts.error}`);
    console.log(`Run log: ${logPath}`);
    console.log("Re-run with --apply to write Contract End Date only.");
    return;
  }

  // APPLY
  const toUpdate = plans.filter((p) => p.status === "ready");
  console.log(`Applying ${toUpdate.length} updates in batches of ${BATCH_SIZE}...`);

  for (let i = 0; i < toUpdate.length; i += BATCH_SIZE) {
    const chunk = toUpdate.slice(i, i + BATCH_SIZE);
    try {
      await client.updateBatch(
        chunk.map((p) => ({
          id: p.recordId,
          fields: { [FIELD_CED]: p.newValue },
        })),
      );
      for (const p of chunk) {
        p.status = "updated";
        counts.updated++;
      }
      console.log(
        `  batch ${Math.floor(i / BATCH_SIZE) + 1}: updated ${chunk.length} (${i + chunk.length}/${toUpdate.length})`,
      );
    } catch (err) {
      console.error(`  batch failed: ${err.message}`);
      for (const p of chunk) {
        p.status = "error";
        p.detail = err.message.slice(0, 200);
        counts.error++;
      }
    }
  }

  for (const p of plans) {
    logRows.push({
      NMI: p.nmi,
      "record id": p.recordId,
      "old value": p.oldValue,
      "new value": p.newValue,
      status: p.status === "ready" ? "error" : p.status,
      detail: p.detail,
      "match type": p.matchType,
    });
  }
  writeCsv(
    logPath,
    ["NMI", "record id", "old value", "new value", "status", "detail", "match type"],
    logRows,
  );

  console.log("");
  console.log("--- Summary ---");
  console.log(`matched:             ${counts.matched}`);
  console.log(`updated:             ${counts.updated}`);
  console.log(`skipped-ambiguous:   ${counts["skipped-ambiguous"]}`);
  console.log(`skipped-drift:       ${counts["skipped-drift"]}`);
  console.log(`not-found:           ${counts["not-found"]}`);
  console.log(`error:               ${counts.error}`);
  console.log(`Run log: ${logPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
