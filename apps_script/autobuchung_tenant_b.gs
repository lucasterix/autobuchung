/***********************
 * PflegeWeb AutoBuchung – Tenant A
 *
 * - API-Key: wird in den Script Properties gespeichert (NICHT im Code).
 *   Einmal beim ersten Nutzen über "API-Key setzen…" eintippen.
 * - Retry: transiente 502/503/504/Timeout werden intern wiederholt.
 * - Progress-Toast: zeigt live, welcher Batch gerade läuft.
 * - Log-Tab: jede auffällige Zeile (errors, ambiguous, multiple_invoices,
 *   duplicate, …) wird im Tab "AutoBuchung_Log" protokolliert.
 * - STOP Kill-Switch: laufende Batch-Loops lassen sich mit einem Klick stoppen.
 ***********************/

// === TENANT-SPEZIFISCH ===
const TENANT = "B";
const CHUNK_MAX_ROWS = 1200;    // kleiner machen bei 504/Timeout; größer wenn stabil

// === UNIVERSAL ===
const API_BASE       = "http://188.245.172.75";
const MAX_ITERATIONS = 30;     // Safety: wie oft wird im Loop max. gebatcht
const BATCH_SLEEP_MS = 300;    // Pause zwischen Batches

const PROP_API_KEY   = "API_KEY";
const PROP_FROM_DATE = "AUTOBOOK_FROM_DATE";
const PROP_STOP      = "AUTOBOOK_STOP";

const LOG_SHEET_NAME = "AutoBuchung_Log";
const LOG_HEADERS = ["Timestamp", "Tenant", "Action", "Batch", "Row", "Invoice", "Reason / Status", "Details"];

// =========================================================
// MENU
// =========================================================
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  const fromDate = getFromDate_() || "(nicht gesetzt)";
  const stop     = getStop_() ? "ON" : "off";
  const keySet   = getApiKeySafe_() ? "✓" : "✗ FEHLT";

  ui.createMenu("AutoBuchung")
    .addItem("🔑 API-Key setzen… (" + keySet + ")", "setApiKey")
    .addSeparator()
    .addItem("Startdatum setzen… (aktuell: " + fromDate + ")", "setFromDate")
    .addItem("STOP Preview/Commit (" + stop + ")", "toggleStop")
    .addSeparator()
    .addItem("1) Preview ziehen", "previewRun")
    .addItem("2) Commit buchen",  "commitRun")
    .addSeparator()
    .addItem("Log-Tab öffnen", "openLogSheet")
    .addToUi();
}

// =========================================================
// SETUP: API KEY
// =========================================================
function setApiKey() {
  const ui = SpreadsheetApp.getUi();
  const current = getApiKeySafe_() || "";
  const masked  = current ? current.slice(0, 4) + "…" + current.slice(-4) : "(nicht gesetzt)";

  const resp = ui.prompt(
    "API-Key setzen (Tenant " + TENANT + ")",
    "Aktuell: " + masked + "\n\nNeuen Key eingeben (leer lassen = löschen):",
    ui.ButtonSet.OK_CANCEL
  );
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const value = (resp.getResponseText() || "").trim();
  const props = PropertiesService.getScriptProperties();
  if (value === "") {
    props.deleteProperty(PROP_API_KEY);
    ui.alert("🗑️ API-Key gelöscht.");
  } else {
    props.setProperty(PROP_API_KEY, value);
    ui.alert("✅ API-Key gespeichert.");
  }
  onOpen();
}

function getApiKeySafe_() {
  return PropertiesService.getScriptProperties().getProperty(PROP_API_KEY) || "";
}

function requireApiKey_() {
  const key = getApiKeySafe_();
  if (!key) {
    SpreadsheetApp.getUi().alert(
      "❌ Kein API-Key hinterlegt.\n\nBitte zuerst im Menü „AutoBuchung → 🔑 API-Key setzen…“ eintragen."
    );
    return null;
  }
  return key;
}

// =========================================================
// SETUP: STARTDATUM
// =========================================================
function setFromDate() {
  const ui = SpreadsheetApp.getUi();
  const current = getFromDate_() || "";

  const resp = ui.prompt(
    "Startdatum für Preview",
    "Format: YYYY-MM-DD.\nLeer = keinen Filter.\n\nAktuell: " + (current || "(nicht gesetzt)"),
    ui.ButtonSet.OK_CANCEL
  );
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const value = (resp.getResponseText() || "").trim();
  if (value === "") {
    setFromDate_(null);
    ui.alert("✅ Startdatum entfernt.");
  } else if (!isValidIsoDate_(value)) {
    ui.alert("❌ Ungültig. Bitte YYYY-MM-DD, z. B. 2026-03-01");
    return;
  } else {
    setFromDate_(value);
    ui.alert("✅ Startdatum gesetzt: " + value);
  }
  onOpen();
}

function getFromDate_() { return PropertiesService.getDocumentProperties().getProperty(PROP_FROM_DATE); }
function setFromDate_(v) {
  const p = PropertiesService.getDocumentProperties();
  if (!v) p.deleteProperty(PROP_FROM_DATE);
  else p.setProperty(PROP_FROM_DATE, v);
}

function isValidIsoDate_(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + "T00:00:00Z");
  if (isNaN(d.getTime())) return false;
  const y  = d.getUTCFullYear().toString().padStart(4, "0");
  const m  = (d.getUTCMonth() + 1).toString().padStart(2, "0");
  const dd = d.getUTCDate().toString().padStart(2, "0");
  return `${y}-${m}-${dd}` === s;
}

// =========================================================
// SETUP: STOP KILL SWITCH
// =========================================================
function toggleStop() {
  const cur = getStop_();
  setStop_(!cur);
  SpreadsheetApp.getUi().alert(
    "Kill Switch ist jetzt: " + (getStop_() ? "ON (stop)" : "off (läuft)")
  );
  onOpen();
}
function getStop_() { return PropertiesService.getDocumentProperties().getProperty(PROP_STOP) === "1"; }
function setStop_(on) {
  const p = PropertiesService.getDocumentProperties();
  if (on) p.setProperty(PROP_STOP, "1");
  else    p.deleteProperty(PROP_STOP);
}

// =========================================================
// LOG SHEET
// =========================================================
function openLogSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(LOG_SHEET_NAME);
  if (!sh) sh = ensureLogSheet_();
  ss.setActiveSheet(sh);
}

function ensureLogSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(LOG_SHEET_NAME);
  if (sh) return sh;
  sh = ss.insertSheet(LOG_SHEET_NAME);
  sh.appendRow(LOG_HEADERS);
  sh.setFrozenRows(1);
  sh.getRange(1, 1, 1, LOG_HEADERS.length).setFontWeight("bold");
  return sh;
}

function logEntries_(action, batchNo, details) {
  if (!details || !details.length) return;

  const interesting = details.filter(d =>
    d && (d.error || d.reason || d.skipped === true || d.ambiguous)
  );
  if (!interesting.length) return;

  const sh = ensureLogSheet_();
  const ts = new Date();
  const rows = interesting.map(d => [
    ts,
    TENANT,
    action,
    batchNo,
    d.row || d.review_row || "",
    d.invoice || (d.candidates ? d.candidates.join(",") : ""),
    d.error || d.reason || (d.skipped ? "skipped" : ""),
    JSON.stringify(d),
  ]);
  sh.getRange(sh.getLastRow() + 1, 1, rows.length, LOG_HEADERS.length).setValues(rows);
}

// =========================================================
// HTTP
// =========================================================
function buildUrl_(base, params) {
  const pairs = [];
  Object.keys(params).forEach(k => {
    const v = params[k];
    if (v !== null && v !== undefined && String(v).trim() !== "") {
      pairs.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
    }
  });
  return pairs.length ? (base + "?" + pairs.join("&")) : base;
}

/**
 * HTTP-Call mit Retry für transiente Fehler.
 * Retries: 502, 503, 504, sowie Netzwerk-Exceptions.
 * Keine Retries bei: 4xx (außer 408/429), 2xx, JSON-Parsing-Fehler.
 * Gibt Response-Objekt zurück oder null bei endgültigem Fehler (Alert wurde gezeigt).
 */
function callApiJson_(url, label) {
  const ui = SpreadsheetApp.getUi();
  const apiKey = requireApiKey_();
  if (!apiKey) return null;

  const MAX_TRIES = 3;
  let lastErrorMsg = "";

  for (let attempt = 1; attempt <= MAX_TRIES; attempt++) {
    try {
      const res = UrlFetchApp.fetch(url, {
        method: "post",
        headers: { "X-API-Key": apiKey, "X-Tenant": TENANT },
        muteHttpExceptions: true,
      });

      const code = res.getResponseCode();
      const body = res.getContentText();

      if (code >= 200 && code < 300) {
        try { return JSON.parse(body); }
        catch (e) {
          ui.alert(`❌ ${label}: OK (${code}), aber kein JSON:\n\n${body.slice(0, 500)}`);
          return null;
        }
      }

      // Retry bei transienten Server-/Netzwerk-Fehlern
      const retryable = (code === 408 || code === 429 || code === 502 || code === 503 || code === 504);
      if (retryable && attempt < MAX_TRIES) {
        const sleepMs = 1000 * Math.pow(2, attempt - 1); // 1s, 2s
        SpreadsheetApp.getActiveSpreadsheet().toast(
          `Versuch ${attempt}/${MAX_TRIES}: HTTP ${code}, warte ${sleepMs}ms…`, `${label}`, 5
        );
        Utilities.sleep(sleepMs);
        lastErrorMsg = `HTTP ${code}`;
        continue;
      }

      // endgültiger Fehler
      ui.alert(`❌ ${label} Fehler (${code}) [Tenant ${TENANT}]\n\n${tryPrettyJson_(body)}`);
      return null;

    } catch (e) {
      // Netzwerk-Exception (Timeout, DNS, …)
      if (attempt < MAX_TRIES) {
        const sleepMs = 1000 * Math.pow(2, attempt - 1);
        SpreadsheetApp.getActiveSpreadsheet().toast(
          `Versuch ${attempt}/${MAX_TRIES}: Netzwerkfehler, warte ${sleepMs}ms…`, `${label}`, 5
        );
        Utilities.sleep(sleepMs);
        lastErrorMsg = String(e);
        continue;
      }
      ui.alert(`❌ ${label} Exception nach ${MAX_TRIES} Versuchen [Tenant ${TENANT}]\n\n${e}`);
      return null;
    }
  }

  ui.alert(`❌ ${label} gab nach ${MAX_TRIES} Versuchen auf: ${lastErrorMsg}`);
  return null;
}

function tryPrettyJson_(txt) {
  try { return JSON.stringify(JSON.parse(txt), null, 2); } catch (_) { return txt; }
}

// =========================================================
// PREVIEW
// =========================================================
function previewRun() {
  const ui = SpreadsheetApp.getUi();
  if (!requireApiKey_()) return;

  if (getStop_()) {
    ui.alert("⛔ Kill Switch ist ON – Preview wird nicht gestartet.");
    return;
  }

  const fromDate = getFromDate_() || "";
  let totals = { moved: 0, skipped: 0, ambiguous: 0, errors: 0 };
  let batches = 0;
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  for (let i = 1; i <= MAX_ITERATIONS; i++) {
    if (getStop_()) break;

    ss.toast(`Batch ${i} läuft… (bisher moved=${totals.moved})`, "Preview", 10);

    const url = buildUrl_(API_BASE + "/api/bank/preview", {
      dry_run: "false",
      max_rows: CHUNK_MAX_ROWS,
      from_date: fromDate
    });

    const result = callApiJson_(url, `Preview (Batch ${i})`);
    if (!result) return; // Fehler wurde schon angezeigt
    batches = i;

    totals.moved     += Number(result.moved || 0);
    totals.skipped   += Number(result.skipped || 0);
    totals.ambiguous += Number(result.ambiguous || 0);
    totals.errors    += Number(result.errors || 0);

    logEntries_("preview", i, result.details || []);

    if (Number(result.moved || 0) <= 0) break;

    Utilities.sleep(BATCH_SLEEP_MS);
  }

  const note = getStop_() ? "\n\n⛔ Vorzeitig gestoppt (Kill Switch ON)." : "";
  ui.alert(
    `✅ Preview fertig [Tenant ${TENANT}]\n` +
    `moved=${totals.moved}\n` +
    `skipped=${totals.skipped}\n` +
    `ambiguous=${totals.ambiguous}\n` +
    `errors=${totals.errors}\n` +
    `batches=${batches}` + note
  );
}

// =========================================================
// COMMIT
// =========================================================
function commitRun() {
  const ui = SpreadsheetApp.getUi();
  if (!requireApiKey_()) return;

  if (getStop_()) {
    ui.alert("⛔ Kill Switch ist ON – Commit wird nicht gestartet.");
    return;
  }

  let totals = { booked: 0, skipped: 0, errors: 0 };
  let batches = 0;
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  for (let i = 1; i <= MAX_ITERATIONS; i++) {
    if (getStop_()) break;

    ss.toast(`Batch ${i} läuft… (bisher gebucht=${totals.booked})`, "Commit", 10);

    const url = buildUrl_(API_BASE + "/api/bank/commit", {
      dry_run: "false",
      max_rows: CHUNK_MAX_ROWS
    });

    const result = callApiJson_(url, `Commit (Batch ${i})`);
    if (!result) return;
    batches = i;

    totals.booked  += Number(result.booked || 0);
    totals.skipped += Number(result.skipped || 0);
    totals.errors  += Number(result.errors || 0);

    logEntries_("commit", i, result.details || []);

    if (Number(result.booked || 0) <= 0) break;

    Utilities.sleep(BATCH_SLEEP_MS);
  }

  const note = getStop_() ? "\n\n⛔ Vorzeitig gestoppt (Kill Switch ON)." : "";
  ui.alert(
    `✅ Commit fertig [Tenant ${TENANT}]\n` +
    `booked=${totals.booked}\n` +
    `skipped=${totals.skipped}\n` +
    `errors=${totals.errors}\n` +
    `batches=${batches}` + note
  );
}
