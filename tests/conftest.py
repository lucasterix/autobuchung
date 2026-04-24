"""
Pytest-Konfiguration. Setzt Pflicht-Env-Variablen für Tenants so, dass das
Modul `app.bank_import` ohne /etc/autobuchung.env importiert werden kann.
"""
import os

_REQUIRED = {
    "A_PATTI_EMAIL": "test@example.com",
    "A_PATTI_PASSWORD": "x",
    "A_GSHEET_ID": "sheet-a",
    "A_GOOGLE_APPLICATION_CREDENTIALS": "/tmp/does-not-exist-a.json",
    "B_PATTI_EMAIL": "test@example.com",
    "B_PATTI_PASSWORD": "x",
    "B_GSHEET_ID": "sheet-b",
    "B_GOOGLE_APPLICATION_CREDENTIALS": "/tmp/does-not-exist-b.json",
}
for k, v in _REQUIRED.items():
    os.environ.setdefault(k, v)
