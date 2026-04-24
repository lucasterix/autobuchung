# Autobuchung

FastAPI-Backend für die Bank-Autobuchung. Wird vom Google Apps Script in den
Buchhaltungs-Spreadsheets via `POST /api/bank/preview` und `POST /api/bank/commit`
aufgerufen. Multi-Tenant über `X-Tenant`-Header (A / B).

Vorher Teil von Pflegekreuzer (`app/routes/bank_import.py`), seit 04/2026 eigenständig.

## Endpoints

- `POST /api/bank/preview` — verschiebt offene Transactions ins Review-Tab
- `POST /api/bank/commit` — bucht freigegebene Review-Zeilen via Patti
- `POST /api/bank/import` — Einzelbuchung per Rechnungsnummer
- `GET /health`

Auth: `X-API-Key`-Header gegen `{TENANT}_BANK_API_KEY`.

## Lokal starten

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env && $EDITOR .env
AUTOBUCHUNG_ENV_FILE=$PWD/.env uvicorn app.main:app --reload --port 8001
```

## ENV

Geladen aus `/etc/autobuchung.env` (per `AUTOBUCHUNG_ENV_FILE` überschreibbar).
Siehe [`.env.example`](.env.example).

Google-Service-Account-JSON unter
`/opt/autobuchung/keys/google-service-account.json` (oder per
`{TENANT}_GOOGLE_APPLICATION_CREDENTIALS` umbiegen). Der Service-Account
braucht Editor-Rechte auf den Tenant-Spreadsheets.

## Deployment

Server: `root@188.245.172.75:/opt/autobuchung/`, systemd-Service `autobuchung`,
uvicorn auf Port 8001. Nginx routet `http://188.245.172.75/api/bank/*` dorthin
(restliche Pfade weiterhin an pflegeweb auf 8000).
