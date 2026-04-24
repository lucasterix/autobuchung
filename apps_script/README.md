# Apps Script Setup

Je Tenant ein Script, das an das Google-Sheet des Tenants gebunden ist
(Extensions → Apps Script). Beide Scripts sind bis auf die Konstanten
`TENANT` und `CHUNK_MAX_ROWS` identisch.

## Installation

1. In das Spreadsheet des Tenants öffnen → **Extensions → Apps Script**
2. Alten Code löschen, Inhalt der passenden Datei hier hineinkopieren:
   - Tenant A → [`autobuchung_tenant_a.gs`](autobuchung_tenant_a.gs)
   - Tenant B → [`autobuchung_tenant_b.gs`](autobuchung_tenant_b.gs)
3. Speichern. Reload des Spreadsheets → Menü **AutoBuchung** erscheint.
4. Einmalig **AutoBuchung → 🔑 API-Key setzen…** ausführen und den
   tenant-spezifischen `{A|B}_BANK_API_KEY` eintragen.
5. Optional: **Startdatum setzen…** (z. B. `2026-01-01`) um alte Monate
   zu ignorieren.

## Menü

| Eintrag                        | Wirkung |
|--------------------------------|---------|
| 🔑 API-Key setzen…             | Speichert API-Key in den Script Properties. Nicht mehr im Code. |
| Startdatum setzen…             | Filter: Preview ignoriert alles vor diesem Datum. |
| STOP Preview/Commit            | Kill-Switch. Laufende Batch-Loops brechen ab. |
| 1) Preview ziehen              | Offene Transactions → Review-Tab. |
| 2) Commit buchen               | Review-Zeilen mit Status `AutoBuchung` → Patti buchen. |
| Log-Tab öffnen                 | Springt zum Tab `AutoBuchung_Log` (wird automatisch angelegt). |

## Was neu ist

- **API-Key im Code entfernt** → liegt jetzt in Script Properties.
  Beim Teilen/Screenshot des Sheets läuft nichts nach außen.
- **Retry** bei 502/503/504/Timeout: 3 Versuche mit exponentialem Backoff
  (1 s, 2 s). Frühere Abbrüche durch Gateway-Timeouts entfallen damit.
- **Progress-Toast** während Batches: „Batch 3 läuft… (bisher gebucht=1500)".
- **Log-Tab `AutoBuchung_Log`**: jede Zeile mit `error`, `reason` oder
  `skipped=true` wird mit Timestamp, Invoice, Grund und JSON-Details
  protokolliert. Erleichtert das Nachvollziehen von Fehlern.
- **Einheitliches UX** über beide Tenants: STOP-Switch + MAX_ITERATIONS
  sind jetzt in beiden Scripts vorhanden.

## Properties

| Key                    | Scope    | Was ist drin |
|------------------------|----------|--------------|
| `API_KEY`              | Script   | `{A|B}_BANK_API_KEY` aus `/etc/autobuchung.env` |
| `AUTOBOOK_FROM_DATE`   | Document | `YYYY-MM-DD` oder fehlt |
| `AUTOBOOK_STOP`        | Document | `"1"` wenn Kill-Switch ON |

Wenn ein anderer User des Sheets den Kill-Switch benutzt, betrifft das nur
das Document (Spreadsheet), nicht den API-Key (Script-level).

## Tuning

- Bei regelmäßigen **504 Gateway Timeouts**: `CHUNK_MAX_ROWS` verkleinern
  (z. B. 200). Weniger Arbeit pro Request, mehr Batches.
- Bei sehr großer Source-Tabelle und stabilem Durchsatz: `CHUNK_MAX_ROWS`
  auf 1000–2000 hochschrauben.
