import logging
import os

from fastapi import FastAPI


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
)
logger = logging.getLogger("autobuchung")

from .bank_import import router as bank_router, _load_cfg_for_tenant  # noqa: E402

app = FastAPI(title="Autobuchung")
app.include_router(bank_router)


def _parse_tenants_env() -> list[str]:
    raw = (os.environ.get("AUTOBUCHUNG_TENANTS") or "A,B").strip()
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


@app.on_event("startup")
def validate_tenants_on_startup() -> None:
    """
    Lädt beim Start jeden in AUTOBUCHUNG_TENANTS aufgeführten Tenant und prüft,
    dass alle Pflicht-Env-Variablen gesetzt sind und die Google-Credentials-
    Datei existiert. Fehler werden laut geloggt, damit sie beim Deploy
    auffallen – der Service startet aber weiter, damit gesunde Tenants
    unabhängig ihrer Geschwister erreichbar bleiben.
    """
    tenants = _parse_tenants_env()
    for t in tenants:
        try:
            cfg = _load_cfg_for_tenant(t)
        except Exception as e:
            logger.error("startup tenant=%s config error: %s", t, e)
            continue

        if not os.path.isfile(cfg.google_application_credentials):
            logger.error(
                "startup tenant=%s: google creds file not found at %s",
                t, cfg.google_application_credentials,
            )
            continue

        logger.info(
            "startup tenant=%s OK (sheet=%s, patti=%s)",
            t, cfg.spreadsheet_id, cfg.patti_base,
        )


@app.get("/health")
def health():
    return {"ok": True}
