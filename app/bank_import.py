from __future__ import annotations

import hashlib
import hmac
import logging
import os
import random
import re
import threading
import time as time_mod
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Optional
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import requests
from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Optional: .env laden
# AUTOBUCHUNG_ENV_FILE überschreibt den Standardpfad (/etc/autobuchung.env).
try:
    from dotenv import load_dotenv  # type: ignore
    _env_path = os.environ.get("AUTOBUCHUNG_ENV_FILE", "/etc/autobuchung.env")
    load_dotenv(_env_path)
except Exception:
    pass


logger = logging.getLogger("autobuchung")

router = APIRouter(prefix="/api/bank", tags=["bank"])

BERLIN = ZoneInfo("Europe/Berlin")

# =========================
# Status-Konstanten
# =========================
STATUS_REVIEW = "review"
STATUS_AUTOBUCHUNG = "AutoBuchung"
STATUS_GEBUCHT = "Gebucht"
STATUS_UNSICHER = "unsicher"
STATUS_BEIHILFE_UNBUCHBAR = "Beihilfe (unbuchbar)"

# Parser-Ausschlüsse
PURPOSE_BLOCKLIST = ("fahrtkostenerstattung", "auslagenerstattung")

# Tenant A: Jahreszahlen skippen (2000-2050)
YEAR_MIN = 2000
YEAR_MAX = 2050

# Google-Service-Account Tokens leben 1h. Wir cachen knapp darunter.
_SHEETS_SERVICE_TTL = 50 * 60

# Patti-Auth-Probe: definitiv ungültige Rechnungsnummer, nur zum Auth-Check.
_AUTH_PROBE_INVOICE = "__autobuchung_auth_probe__"


# =========================================================
# TENANT CONFIG
# =========================================================

@dataclass(frozen=True)
class TenantConfig:
    tenant: str

    # auth
    bank_api_key: str

    # patti
    patti_base: str
    patti_email: str
    patti_password: str
    http_timeout: float

    # sheets
    spreadsheet_id: str
    source_tab: str
    review_tab: str
    google_application_credentials: str

    # Source: Transactions Layout:
    # A=Date, B=Description, C=Category, D=Amount, ... I=Transaction ID/Marker, K=Status
    source_range_a1: str
    source_status_idx: int
    source_date_idx: int
    source_amount_idx: int
    source_purpose_idx: int
    source_marker_idx: int
    source_status_col_letter: str

    # Review Layout:
    # A=SourceRow, B=Status, C=Valutadatum, D=Betrag, E=Name, F=Verwendungszweck, G=Rechnungsnummer, H=SourceRow
    review_range_a1: str

    # parsing
    invoice_re: re.Pattern[str]


def _env_required(key: str) -> str:
    v = (os.environ.get(key) or "").strip()
    if not v:
        raise HTTPException(status_code=500, detail=f"Missing env: {key}")
    return v


def _env_optional(key: str, default: str) -> str:
    v = os.environ.get(key)
    return (v.strip() if v is not None else default)


def _get_tenant_from_request(request: Request) -> str:
    t = (request.headers.get("X-Tenant") or "").strip()
    if t:
        return t
    return _env_optional("BANK_DEFAULT_TENANT", "A").strip() or "A"


def _load_cfg_for_tenant(tenant: str) -> TenantConfig:
    t = (tenant or "").strip().upper()
    if not t:
        raise HTTPException(status_code=400, detail="Missing tenant")
    prefix = f"{t}_"

    invoice_re_a = re.compile(r"(?<!\d)(\d{4}|\d{5})(?!\d)")
    invoice_re_b = re.compile(r"(?<!\d)([7-9]\d{4})(?!\d)")
    invoice_re = invoice_re_a if t == "A" else invoice_re_b

    return TenantConfig(
        tenant=t,
        bank_api_key=_env_optional(prefix + "BANK_API_KEY", "").strip(),
        patti_base=_env_optional(prefix + "PATTI_BASE", "https://patti.app").rstrip("/"),
        patti_email=_env_required(prefix + "PATTI_EMAIL"),
        patti_password=_env_required(prefix + "PATTI_PASSWORD"),
        http_timeout=float(_env_optional(prefix + "PATTI_HTTP_TIMEOUT", "20")),
        spreadsheet_id=_env_required(prefix + "GSHEET_ID"),
        source_tab=_env_optional(prefix + "GSHEET_SOURCE_TAB", "Transactions"),
        review_tab=_env_optional(prefix + "GSHEET_REVIEW_TAB", "AutoBuchung"),
        google_application_credentials=_env_required(prefix + "GOOGLE_APPLICATION_CREDENTIALS"),
        source_range_a1="A2:K",
        source_status_idx=10,
        source_date_idx=0,
        source_amount_idx=3,
        source_purpose_idx=1,
        source_marker_idx=8,
        source_status_col_letter="K",
        review_range_a1="A2:H",
        invoice_re=invoice_re,
    )


def _load_cfg(request: Request) -> TenantConfig:
    return _load_cfg_for_tenant(_get_tenant_from_request(request))


# =========================================================
# API KEY GUARD
# =========================================================

def _require_api_key(cfg: TenantConfig, x_api_key: Optional[str]) -> None:
    if not cfg.bank_api_key:
        return
    provided = (x_api_key or "").strip()
    if not provided or not hmac.compare_digest(provided, cfg.bank_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")


# =========================================================
# HELPERS: Blank / Money / Dates / Marker
# =========================================================

def _is_blank_cell(v: Any) -> bool:
    if v is None:
        return True
    return str(v).strip() == ""


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace(" ", " ")).strip()


def _parse_amount_to_cents(value: Any) -> int:
    """
    Robust gegen:
      - "100,55" (de)
      - "100.55" (en)
      - "10,321" (en thousands) -> 10321
      - "10.321,00" (de thousands + decimals)
      - führendes "'" (Text erzwungen)
    """
    if value is None:
        raise ValueError("amount is empty")

    s = str(value).strip()
    if s == "":
        raise ValueError("amount is empty")

    if s.startswith("'"):
        s = s[1:].strip()

    s = s.replace(" ", "")

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        parts = s.split(",")
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and len(parts[1]) == 3:
            s = parts[0] + parts[1]
        else:
            s = s.replace(",", ".")

    try:
        dec = Decimal(s)
    except InvalidOperation as e:
        raise ValueError(f"invalid amount '{value}'") from e

    return int((dec * 100).quantize(Decimal("1")))


def _format_cents_eu_as_text(amount_cents: int) -> str:
    sign = "-" if amount_cents < 0 else ""
    n = abs(amount_cents)
    euros = n // 100
    cents = n % 100
    euros_str = f"{euros:,}".replace(",", ".")
    return "'" + f"{sign}{euros_str},{cents:02d}"


def _parse_sheet_date(value: Any) -> date:
    if value is None:
        raise ValueError("date is empty")
    s = str(value).strip()
    if not s:
        raise ValueError("date is empty")

    if re.match(r"^\d{1,2}\.\d{1,2}\.\d{4}$", s):
        return datetime.strptime(s, "%d.%m.%Y").date()

    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    try:
        return datetime.fromisoformat(s).date()
    except Exception as e:
        raise ValueError(f"unsupported date format '{s}'") from e


def _make_fallback_marker(vdate: date, amount_cents: int, purpose: str) -> str:
    raw = f"{vdate.isoformat()}|{amount_cents}|{_normalize_ws(purpose)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _note_with_marker(purpose: str, marker: str) -> str:
    p = (purpose or "").strip()
    return f"{p}\n[AUTOBOOK:{marker}]".strip()


# =========================================================
# PARSER
# =========================================================

@dataclass
class ParseResult:
    invoice_no: Optional[str]
    candidates: list[str]
    skipped_reason: Optional[str]


RE_DATE_DDMMYYYY = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
RE_IBAN = re.compile(r"\bIBAN[:\s]*[A-Z]{2}[0-9A-Z]{10,}\b", re.IGNORECASE)
RE_BIC = re.compile(r"\bBIC[:\s]*[A-Z0-9]{8,11}\b", re.IGNORECASE)


def _sanitize_purpose_for_invoice(text: str) -> str:
    s = text or ""
    s = RE_DATE_DDMMYYYY.sub(" ", s)
    s = RE_IBAN.sub(" ", s)
    s = RE_BIC.sub(" ", s)
    return s


def _extract_invoice_candidates(purpose: str, cfg: TenantConfig) -> list[str]:
    raw = cfg.invoice_re.findall(purpose or "")
    out: list[str] = []

    for token in raw:
        if cfg.tenant == "A":
            if len(token) == 5 and not token.startswith("1"):
                continue
            n = int(token)
            if YEAR_MIN <= n <= YEAR_MAX:
                continue
        out.append(token)

    dedup: list[str] = []
    seen = set()
    for x in out:
        if x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup


def parse_purpose_for_invoice(purpose: Optional[str], cfg: TenantConfig) -> ParseResult:
    raw_text = (purpose or "").strip()
    text = _sanitize_purpose_for_invoice(raw_text) if cfg.tenant == "A" else raw_text
    low = text.lower()

    if any(x in low for x in PURPOSE_BLOCKLIST):
        return ParseResult(invoice_no=None, candidates=[], skipped_reason="blocked_purpose")

    candidates = _extract_invoice_candidates(text, cfg)

    if len(candidates) == 0:
        return ParseResult(invoice_no=None, candidates=[], skipped_reason="no_candidate")

    if len(candidates) > 1:
        return ParseResult(invoice_no=None, candidates=candidates, skipped_reason="multiple_candidates_skip")

    return ParseResult(invoice_no=candidates[0], candidates=candidates, skipped_reason=None)


# =========================================================
# GOOGLE SHEETS CLIENT + RETRIES (cached per tenant)
# =========================================================

_sheets_cache: dict[str, tuple[float, Any]] = {}
_sheets_cache_lock = threading.Lock()


def _sheets_service(cfg: TenantConfig):
    now = time_mod.monotonic()
    with _sheets_cache_lock:
        cached = _sheets_cache.get(cfg.tenant)
        if cached and (now - cached[0]) < _SHEETS_SERVICE_TTL:
            return cached[1]

    try:
        creds = service_account.Credentials.from_service_account_file(
            cfg.google_application_credentials,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    except FileNotFoundError:
        logger.error(
            "google creds file missing for tenant %s at %s",
            cfg.tenant, cfg.google_application_credentials,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Google service account file not found: {cfg.google_application_credentials}",
        )
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    with _sheets_cache_lock:
        _sheets_cache[cfg.tenant] = (now, svc)
    return svc


def _is_retryable_network_error(e: Exception) -> bool:
    if isinstance(e, HttpError):
        status = getattr(e.resp, "status", None)
        return status in (429, 500, 502, 503, 504)
    if isinstance(e, (TimeoutError, ConnectionError)):
        return True
    if isinstance(
        e,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    return False


def _retry(fn, *, tries: int = 6, base_sleep: float = 0.6, max_sleep: float = 6.0, op_label: str = "op"):
    last: Optional[Exception] = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if not _is_retryable_network_error(e) or i == tries - 1:
                raise
            sleep = min(max_sleep, base_sleep * (2 ** i)) * (0.75 + random.random() * 0.5)
            logger.warning("retry %s (attempt %d/%d) after %s: %s", op_label, i + 1, tries, type(e).__name__, e)
            time_mod.sleep(sleep)
    if last:
        raise last
    raise RuntimeError("retry failed without exception")


def _get_values(svc, spreadsheet_id: str, range_a1: str) -> list[list[Any]]:
    def _do():
        sheet = svc.spreadsheets()
        res = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_a1).execute()
        return res.get("values", [])
    return _retry(_do, op_label=f"sheets.get {range_a1}")


def _update_values(svc, spreadsheet_id: str, range_a1: str, values: list[list[Any]]):
    def _do():
        sheet = svc.spreadsheets()
        return sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_a1,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
    return _retry(_do, op_label=f"sheets.update {range_a1}")


def _append_values(svc, spreadsheet_id: str, range_a1: str, values: list[list[Any]]):
    def _do():
        sheet = svc.spreadsheets()
        return sheet.values().append(
            spreadsheetId=spreadsheet_id,
            range=range_a1,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    return _retry(_do, op_label=f"sheets.append {range_a1}")


def _set_review_status_with_fallback(
    svc,
    cfg: TenantConfig,
    review_sheet_row: int,
    status_text: str,
    *,
    fallback_status: str = STATUS_UNSICHER,
    fallback_prefix_in_purpose: Optional[str] = None,
) -> None:
    """
    Status in Spalte B setzen. Wenn Sheet eine Dropdown-Validation hat und
    status_text dort nicht als Option existiert, fällt die API mit HttpError
    zurück – dann setzen wir fallback_status und notieren den gewünschten
    Status als Prefix im Verwendungszweck (Spalte F).
    """
    try:
        _update_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!B{review_sheet_row}", [[status_text]])
        return
    except HttpError as exc:
        logger.info(
            "review status fallback for tenant %s row %d (wanted=%s, using=%s): %s",
            cfg.tenant, review_sheet_row, status_text, fallback_status, exc,
        )
        _update_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!B{review_sheet_row}", [[fallback_status]])

        if fallback_prefix_in_purpose:
            try:
                current = _get_values(
                    svc, cfg.spreadsheet_id, f"{cfg.review_tab}!F{review_sheet_row}:F{review_sheet_row}"
                )
                cur_val = ""
                if current and current[0]:
                    cur_val = str(current[0][0] or "")
                new_val = f"{fallback_prefix_in_purpose}{cur_val}".strip()
                _update_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!F{review_sheet_row}", [[new_val]])
            except Exception as sub_exc:
                logger.warning(
                    "review purpose-prefix update failed for tenant %s row %d: %s",
                    cfg.tenant, review_sheet_row, sub_exc,
                )


# =========================================================
# PATTI SESSION (per-tenant with per-tenant lock)
# =========================================================

_sessions: dict[str, requests.Session] = {}
_session_locks: dict[str, threading.Lock] = {}
_session_locks_guard = threading.Lock()


def _lock_for(tenant: str) -> threading.Lock:
    with _session_locks_guard:
        lk = _session_locks.get(tenant)
        if lk is None:
            lk = threading.Lock()
            _session_locks[tenant] = lk
        return lk


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return s


def _extract_hidden_csrf(login_html: str) -> Optional[str]:
    m = re.search(r'name="_token"\s+value="([^"]+)"', login_html)
    return m.group(1) if m else None


def _get_xsrf_token(session: requests.Session) -> Optional[str]:
    xsrf = session.cookies.get("XSRF-TOKEN")
    if not xsrf:
        return None
    return unquote(xsrf)


def _verify_auth(cfg: TenantConfig, session: requests.Session) -> None:
    """
    Bestätigt, dass die Session wirklich authentifiziert ist. Rohes POST /login
    kann auch bei Falsch-Credentials einen 200/302 liefern und uns glauben lassen,
    wir seien eingeloggt. Ein Probe-Call auf einen geschützten Endpoint entlarvt
    das (401/403/419 bei nicht-auth, alles andere gilt als authentifiziert).
    """
    try:
        r = session.get(
            f"{cfg.patti_base}/api/v1/invoices",
            params={"number": _AUTH_PROBE_INVOICE},
            timeout=cfg.http_timeout,
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Patti auth probe network error: {e}")

    if r.status_code in (401, 403, 419):
        raise HTTPException(
            status_code=502,
            detail=f"Patti login did not produce an authenticated session (probe HTTP {r.status_code})",
        )


def _login(cfg: TenantConfig, session: requests.Session) -> None:
    logger.info("patti login starting for tenant %s", cfg.tenant)
    try:
        r = session.get(f"{cfg.patti_base}/login", timeout=cfg.http_timeout)
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Patti login page network error: {e}")

    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Patti login page failed: {r.status_code} {r.text[:200]}")

    hidden = _extract_hidden_csrf(r.text)
    xsrf = _get_xsrf_token(session)

    if not hidden and not xsrf:
        raise HTTPException(
            status_code=502,
            detail="Could not extract CSRF token (hidden _token and XSRF cookie missing)",
        )

    payload = {
        **({"_token": hidden} if hidden else {}),
        "email": cfg.patti_email,
        "password": cfg.patti_password,
    }

    headers = {"Origin": cfg.patti_base, "Referer": f"{cfg.patti_base}/login"}
    if xsrf:
        headers["X-XSRF-TOKEN"] = xsrf

    try:
        r = session.post(
            f"{cfg.patti_base}/login",
            data=payload,
            headers=headers,
            allow_redirects=False,
            timeout=cfg.http_timeout,
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Patti login POST network error: {e}")

    if r.status_code not in (200, 302, 303):
        raise HTTPException(status_code=502, detail=f"Patti login failed: {r.status_code} {r.text[:200]}")

    if r.status_code in (302, 303) and r.headers.get("Location"):
        loc = r.headers["Location"]
        if loc.startswith("/"):
            loc = cfg.patti_base + loc
        # Redirect folgen, damit Session-Cookies nachgezogen werden. Fehler dabei
        # sind nicht fatal – _verify_auth entscheidet final.
        try:
            session.get(loc, timeout=cfg.http_timeout)
        except requests.exceptions.RequestException as e:
            logger.info("patti post-login redirect follow failed for tenant %s: %s", cfg.tenant, e)

    session.headers.update({"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
    xsrf = _get_xsrf_token(session)
    if xsrf:
        session.headers["X-XSRF-TOKEN"] = xsrf

    _verify_auth(cfg, session)
    logger.info("patti login OK for tenant %s", cfg.tenant)


def _get_session(cfg: TenantConfig) -> requests.Session:
    with _lock_for(cfg.tenant):
        s = _sessions.get(cfg.tenant)
        if s is None:
            s = _make_session()
            _login(cfg, s)
            _sessions[cfg.tenant] = s
        return s


def _reset_session(cfg: TenantConfig) -> requests.Session:
    """Neuen Login erzwingen (Session invalid/expired)."""
    with _lock_for(cfg.tenant):
        s = _make_session()
        _login(cfg, s)
        _sessions[cfg.tenant] = s
        return s


def _parse_retry_after(value: Optional[str], default: float) -> float:
    """HTTP Retry-After-Header: entweder Sekunden (int) oder HTTP-Date; wir nehmen nur Sekunden."""
    if not value:
        return default
    try:
        sec = float(value.strip())
        return max(0.0, min(sec, 60.0))  # deckeln bei 60s, damit Request nicht ewig hängt
    except (TypeError, ValueError):
        return default


def _request(cfg: TenantConfig, method: str, url: str, *, params=None, json=None) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        s = _get_session(cfg)
        try:
            r = s.request(method, url, params=params, json=json, timeout=cfg.http_timeout)
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            last_exc = e
            if attempt == 2:
                raise HTTPException(status_code=502, detail=f"Patti network error: {e}")
            logger.warning(
                "patti %s %s network error (attempt %d/3): %s",
                method, url, attempt + 1, e,
            )
            time_mod.sleep(0.5 * (2 ** attempt))
            continue

        if r.status_code == 429:
            if attempt == 2:
                return r
            wait = _parse_retry_after(r.headers.get("Retry-After"), default=2.0 * (2 ** attempt))
            logger.warning(
                "patti %s %s rate-limited (429, attempt %d/3), sleeping %.1fs",
                method, url, attempt + 1, wait,
            )
            time_mod.sleep(wait)
            continue

        if r.status_code in (401, 403, 419):
            if attempt == 2:
                return r
            logger.info(
                "patti %s %s -> %d, re-authenticating tenant %s",
                method, url, r.status_code, cfg.tenant,
            )
            _reset_session(cfg)
            continue

        return r

    # Fallback (sollte nicht erreicht werden)
    if last_exc:
        raise HTTPException(status_code=502, detail=f"Patti network error: {last_exc}")
    raise HTTPException(status_code=502, detail="Patti request exhausted retries")


def _lookup_invoice_by_number(cfg: TenantConfig, invoice_no: str) -> dict:
    r = _request(cfg, "GET", f"{cfg.patti_base}/api/v1/invoices", params={"number": invoice_no})
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Invoice lookup failed: {r.status_code} {r.text[:200]}")
    try:
        data = r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Invoice lookup returned non-JSON")
    items = data.get("data") or data.get("invoices") or []
    if not items:
        raise HTTPException(status_code=404, detail=f"Invoice not found: {invoice_no}")
    if len(items) > 1:
        raise HTTPException(
            status_code=409,
            detail=f"Multiple invoices found for number {invoice_no} ({len(items)}). Skipping auto-booking.",
        )
    return items[0]


def _lookup_invoice_detail(cfg: TenantConfig, invoice_id: int) -> dict:
    r = _request(cfg, "GET", f"{cfg.patti_base}/api/v1/invoices/{invoice_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Invoice detail failed: {r.status_code} {r.text[:200]}")
    try:
        return r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Invoice detail returned non-JSON")


def _pick_invoice_version_id(invoice_detail: dict) -> int:
    versions = invoice_detail.get("versions")
    if not isinstance(versions, list) or not versions:
        raise HTTPException(status_code=502, detail="Invoice detail contains no versions")

    if len(versions) != 1:
        raise HTTPException(
            status_code=409,
            detail=f"Invoice has multiple versions ({len(versions)}). Skipping auto-booking.",
        )

    v = versions[0]
    if isinstance(v, dict) and v.get("id"):
        return int(v["id"])

    raise HTTPException(status_code=502, detail="Could not determine invoice_version_id")


def _invoice_has_marker(invoice_detail: dict, marker: str) -> bool:
    needle = f"[AUTOBOOK:{marker}]"

    def _scan(payments):
        if not isinstance(payments, list):
            return False
        for p in payments:
            if not isinstance(p, dict):
                continue
            if needle in str(p.get("note", "") or ""):
                return True
        return False

    if _scan(invoice_detail.get("payments")):
        return True

    versions = invoice_detail.get("versions") or []
    if isinstance(versions, list):
        for v in versions:
            if isinstance(v, dict) and _scan(v.get("payments")):
                return True

    return False


def _create_payment(cfg: TenantConfig, invoice_version_id: int, booked_at_iso: str, amount_cents: int, note: str) -> dict:
    payload = {
        "id": 0,
        "invoice_version_id": invoice_version_id,
        "booked_at": booked_at_iso,
        "amount": int(amount_cents),
        "note": note or "",
        "type": "sepa",
        "is_draft": False,
        "transaction_id": None,
    }
    r = _request(cfg, "POST", f"{cfg.patti_base}/api/v1/payments", json=payload)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Payment failed: {r.status_code} {r.text[:200]}")
    try:
        return r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Payment returned non-JSON")


def _book_payment_to_patti(cfg: TenantConfig, invoice_no: str, amount_cents: int, value_date: date, purpose: str, marker: str) -> dict:
    booked_at_local = datetime.combine(value_date, time(12, 0), tzinfo=BERLIN)
    booked_at_iso = booked_at_local.isoformat(timespec="seconds")

    inv = _lookup_invoice_by_number(cfg, invoice_no)
    inv_id = inv.get("id")
    if not inv_id:
        raise HTTPException(status_code=502, detail="Invoice lookup returned no id")

    inv_detail = _lookup_invoice_detail(cfg, int(inv_id))

    if _invoice_has_marker(inv_detail, marker):
        raise HTTPException(status_code=409, detail="Duplicate detected (marker already exists in Patti)")

    invoice_version_id = _pick_invoice_version_id(inv_detail)

    note = _note_with_marker(purpose, marker)
    payment = _create_payment(cfg, invoice_version_id, booked_at_iso, amount_cents, note)

    return {
        "invoice_id": int(inv_id),
        "invoice_version_id": invoice_version_id,
        "payment_id": payment.get("id"),
        "amount": payment.get("amount"),
        "booked_at": payment.get("booked_at"),
    }


# =========================================================
# API MODELS
# =========================================================

class PreviewResponse(BaseModel):
    ok: bool
    moved: int
    skipped: int
    ambiguous: int
    errors: int
    details: list[dict]


class CommitResponse(BaseModel):
    ok: bool
    booked: int
    skipped: int
    errors: int
    details: list[dict]


class BankImportRequest(BaseModel):
    invoice_no: str = Field(..., description="Rechnungsnummer")
    amount: int = Field(..., gt=0, description="Betrag in Cent")
    value_date: date = Field(..., description="Valutadatum YYYY-MM-DD")
    purpose: Optional[str] = Field(None, description="Verwendungszweck")


# =========================================================
# READERS / MATCHERS
# =========================================================

def _read_source_row_transactions(cfg: TenantConfig, svc, source_row: int) -> dict:
    r = _get_values(svc, cfg.spreadsheet_id, f"{cfg.source_tab}!A{source_row}:K{source_row}")
    row = r[0] if r and r[0] else []

    date_raw = row[cfg.source_date_idx] if len(row) > cfg.source_date_idx else None
    amount_raw = row[cfg.source_amount_idx] if len(row) > cfg.source_amount_idx else None
    purpose_raw = row[cfg.source_purpose_idx] if len(row) > cfg.source_purpose_idx else ""
    marker_raw = row[cfg.source_marker_idx] if len(row) > cfg.source_marker_idx else ""
    status_raw = row[cfg.source_status_idx] if len(row) > cfg.source_status_idx else ""

    vdate = _parse_sheet_date(date_raw)
    amount_cents = _parse_amount_to_cents(amount_raw)
    purpose = str(purpose_raw or "")
    marker_existing = str(marker_raw).strip() if marker_raw is not None else ""
    status = str(status_raw).replace(" ", " ").strip() if status_raw is not None else ""

    return {
        "vdate": vdate,
        "amount_cents": amount_cents,
        "purpose": purpose,
        "marker_existing": marker_existing,
        "status": status,
    }


def _read_review_row_values(r: list[Any]) -> dict:
    source_row_a = r[0] if len(r) > 0 else None
    status_raw = r[1] if len(r) > 1 else None
    value_date_raw = r[2] if len(r) > 2 else None
    amount_raw = r[3] if len(r) > 3 else None
    name_raw = r[4] if len(r) > 4 else ""
    purpose_raw = r[5] if len(r) > 5 else ""
    invoice_raw = r[6] if len(r) > 6 else ""
    source_row_h = r[7] if len(r) > 7 else None

    status = str(status_raw).replace(" ", " ").strip() if status_raw is not None else ""
    invoice_no = str(invoice_raw).strip() if invoice_raw is not None else ""
    purpose = str(purpose_raw or "")
    name = str(name_raw or "")

    return {
        "source_row_a": source_row_a,
        "source_row_h": source_row_h,
        "status": status,
        "value_date_raw": value_date_raw,
        "amount_raw": amount_raw,
        "purpose": purpose,
        "invoice_no": invoice_no,
        "name": name,
    }


def _source_row_matches_review_data(
    cfg: TenantConfig,
    svc,
    source_row: int,
    *,
    review_vdate: date,
    review_amount_cents: int,
    review_purpose: str,
) -> tuple[bool, Optional[str]]:
    try:
        src = _read_source_row_transactions(cfg, svc, source_row)
    except Exception as e:
        return False, f"read_source_failed: {e}"

    same_date = src["vdate"] == review_vdate
    same_amount = src["amount_cents"] == review_amount_cents
    same_purpose = _normalize_ws(src["purpose"]) == _normalize_ws(review_purpose)

    if same_date and same_amount and same_purpose:
        return True, None

    return False, "source_row_mismatch_after_shift"


def _find_matching_source_row_transactions(
    cfg: TenantConfig,
    svc,
    *,
    review_vdate: date,
    review_amount_cents: int,
    review_purpose: str,
    preferred_source_row: Optional[int] = None,
    source_rows_cache: Optional[list[list[Any]]] = None,
) -> tuple[Optional[int], str]:
    if preferred_source_row:
        # Schneller Preferred-Check bevorzugt aus dem Cache, sonst via Einzel-API-Call.
        ok = False
        if source_rows_cache is not None:
            cache_idx = preferred_source_row - 2
            if 0 <= cache_idx < len(source_rows_cache):
                row = source_rows_cache[cache_idx]
                try:
                    date_raw = row[cfg.source_date_idx] if len(row) > cfg.source_date_idx else None
                    amount_raw = row[cfg.source_amount_idx] if len(row) > cfg.source_amount_idx else None
                    purpose_raw = row[cfg.source_purpose_idx] if len(row) > cfg.source_purpose_idx else ""
                    if (
                        _parse_sheet_date(date_raw) == review_vdate
                        and _parse_amount_to_cents(amount_raw) == review_amount_cents
                        and _normalize_ws(str(purpose_raw or "")) == _normalize_ws(review_purpose)
                    ):
                        ok = True
                except Exception:
                    ok = False
        else:
            ok, _reason = _source_row_matches_review_data(
                cfg,
                svc,
                preferred_source_row,
                review_vdate=review_vdate,
                review_amount_cents=review_amount_cents,
                review_purpose=review_purpose,
            )
        if ok:
            return preferred_source_row, "preferred_row_still_matches"

    if source_rows_cache is not None:
        rows = source_rows_cache
    else:
        rows = _get_values(svc, cfg.spreadsheet_id, f"{cfg.source_tab}!{cfg.source_range_a1}")
    matches: list[int] = []

    want_purpose = _normalize_ws(review_purpose)

    for idx, row in enumerate(rows):
        sheet_row = idx + 2
        try:
            date_raw = row[cfg.source_date_idx] if len(row) > cfg.source_date_idx else None
            amount_raw = row[cfg.source_amount_idx] if len(row) > cfg.source_amount_idx else None
            purpose_raw = row[cfg.source_purpose_idx] if len(row) > cfg.source_purpose_idx else ""

            vdate = _parse_sheet_date(date_raw)
            amount_cents = _parse_amount_to_cents(amount_raw)
            purpose = _normalize_ws(str(purpose_raw or ""))

            if vdate == review_vdate and amount_cents == review_amount_cents and purpose == want_purpose:
                matches.append(sheet_row)
        except Exception:
            continue

    if len(matches) == 1:
        return matches[0], "matched_by_review_data_unique"

    if len(matches) == 0:
        return None, "no_matching_source_row_found"

    return None, f"multiple_matching_source_rows:{len(matches)}"


def _sync_source_status_if_possible(
    cfg: TenantConfig,
    svc,
    *,
    review_sheet_row: int,
    review_vdate: date,
    review_amount_cents: int,
    review_purpose: str,
    source_row_hint: Optional[int],
    target_status: str,
    source_rows_cache: Optional[list[list[Any]]] = None,
) -> dict:
    matched_source_row, sync_reason = _find_matching_source_row_transactions(
        cfg,
        svc,
        review_vdate=review_vdate,
        review_amount_cents=review_amount_cents,
        review_purpose=review_purpose,
        preferred_source_row=source_row_hint,
        source_rows_cache=source_rows_cache,
    )

    if matched_source_row is None:
        return {
            "source_status_synced": False,
            "source_row": None,
            "source_sync_reason": sync_reason,
            "review_row": review_sheet_row,
        }

    _update_values(
        svc,
        cfg.spreadsheet_id,
        f"{cfg.source_tab}!{cfg.source_status_col_letter}{matched_source_row}",
        [[target_status]],
    )

    return {
        "source_status_synced": True,
        "source_row": matched_source_row,
        "source_sync_reason": sync_reason,
        "review_row": review_sheet_row,
    }


# =========================================================
# ENDPOINTS
# =========================================================

@router.get("/status")
def status(tenant: Optional[str] = None, x_api_key: Optional[str] = Header(None)):
    """
    Sanity-Check pro Tenant:
      - alle benötigten ENV-Variablen vorhanden
      - Service-Account-Datei lesbar
    Liefert bei Problem 500 mit sprechendem detail. Braucht gültigen API-Key,
    damit Config-Fehlermeldungen nicht öffentlich exponiert werden.
    """
    if not tenant:
        raise HTTPException(status_code=400, detail="Missing tenant query param")
    cfg = _load_cfg_for_tenant(tenant)
    _require_api_key(cfg, x_api_key)

    creds_ok = os.path.isfile(cfg.google_application_credentials)
    return {
        "ok": creds_ok,
        "tenant": cfg.tenant,
        "patti_base": cfg.patti_base,
        "spreadsheet_id": cfg.spreadsheet_id,
        "source_tab": cfg.source_tab,
        "review_tab": cfg.review_tab,
        "google_credentials_file": cfg.google_application_credentials,
        "google_credentials_readable": creds_ok,
    }


@router.post("/import")
def import_bank_payment(request: Request, req: BankImportRequest, x_api_key: Optional[str] = Header(None)):
    cfg = _load_cfg(request)
    _require_api_key(cfg, x_api_key)

    purpose = req.purpose or "Bankimport"
    marker = _make_fallback_marker(req.value_date, req.amount, purpose)

    logger.info(
        "import tenant=%s invoice=%s amount=%d date=%s marker=%s",
        cfg.tenant, req.invoice_no, req.amount, req.value_date.isoformat(), marker,
    )

    result = _book_payment_to_patti(
        cfg=cfg,
        invoice_no=req.invoice_no,
        amount_cents=req.amount,
        value_date=req.value_date,
        purpose=purpose,
        marker=marker,
    )
    return {"ok": True, "tenant": cfg.tenant, "requested_invoice_no": req.invoice_no, "marker": marker, **result}


@router.post("/preview", response_model=PreviewResponse)
def preview(
    request: Request,
    dry_run: bool = True,
    max_rows: int = 500,
    from_date: Optional[str] = None,
    x_api_key: Optional[str] = Header(None),
):
    cfg = _load_cfg(request)
    _require_api_key(cfg, x_api_key)
    svc = _sheets_service(cfg)

    source_range = f"{cfg.source_tab}!{cfg.source_range_a1}"
    rows = _get_values(svc, cfg.spreadsheet_id, source_range)

    parsed_from_date: Optional[date] = None
    if from_date:
        parsed_from_date = _parse_sheet_date(from_date)

    moved = 0
    skipped = 0
    ambiguous = 0
    errors = 0
    details: list[dict] = []

    logger.info(
        "preview start tenant=%s rows=%d max_rows=%d from_date=%s dry_run=%s",
        cfg.tenant, len(rows), max_rows, from_date or "-", dry_run,
    )

    for idx, r in enumerate(rows):
        if max_rows > 0 and moved >= max_rows:
            break

        sheet_row = idx + 2

        status_val = r[cfg.source_status_idx] if len(r) > cfg.source_status_idx else None
        if not _is_blank_cell(status_val):
            skipped += 1
            continue

        try:
            value_date_raw = r[cfg.source_date_idx] if len(r) > cfg.source_date_idx else None
            amount_raw = r[cfg.source_amount_idx] if len(r) > cfg.source_amount_idx else None
            purpose_raw = r[cfg.source_purpose_idx] if len(r) > cfg.source_purpose_idx else ""

            vdate = _parse_sheet_date(value_date_raw)
            if parsed_from_date and vdate < parsed_from_date:
                skipped += 1
                continue

            amount_cents = _parse_amount_to_cents(amount_raw)
            if amount_cents <= 0:
                skipped += 1
                continue

            purpose = str(purpose_raw or "")

            pr = parse_purpose_for_invoice(purpose, cfg)

            if pr.skipped_reason == "multiple_candidates_skip":
                skipped += 1
                ambiguous += 1
                details.append({
                    "row": sheet_row,
                    "skipped": True,
                    "reason": "multiple_invoices",
                    "candidates": pr.candidates,
                    "tenant": cfg.tenant,
                })
                continue

            if pr.invoice_no is None:
                skipped += 1
                continue

            amount_for_review = _format_cents_eu_as_text(amount_cents)

            review_row = [
                sheet_row,         # A alter SourceRow-Hinweis
                STATUS_REVIEW,     # B Status
                vdate.isoformat(), # C Valutadatum
                amount_for_review, # D Betrag
                "",                # E Name
                purpose,           # F Verwendungszweck
                pr.invoice_no,     # G Rechnungsnummer
                sheet_row,         # H alter SourceRow-Hinweis
            ]

            if not dry_run:
                _append_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!A:H", [review_row])
                _update_values(
                    svc,
                    cfg.spreadsheet_id,
                    f"{cfg.source_tab}!{cfg.source_status_col_letter}{sheet_row}",
                    [[STATUS_REVIEW]],
                )

            moved += 1

        except Exception as e:
            errors += 1
            logger.exception(
                "preview row error tenant=%s row=%d: %s", cfg.tenant, sheet_row, e,
            )
            details.append({"row": sheet_row, "error": str(e), "tenant": cfg.tenant})

    logger.info(
        "preview done tenant=%s moved=%d skipped=%d ambiguous=%d errors=%d",
        cfg.tenant, moved, skipped, ambiguous, errors,
    )

    return PreviewResponse(ok=True, moved=moved, skipped=skipped, ambiguous=ambiguous, errors=errors, details=details)


@router.post("/commit", response_model=CommitResponse)
def commit(
    request: Request,
    dry_run: bool = True,
    max_rows: int = 500,
    x_api_key: Optional[str] = Header(None),
):
    cfg = _load_cfg(request)
    _require_api_key(cfg, x_api_key)
    svc = _sheets_service(cfg)

    review_range = f"{cfg.review_tab}!{cfg.review_range_a1}"
    rows = _get_values(svc, cfg.spreadsheet_id, review_range)

    booked = 0
    skipped = 0
    errors = 0
    details: list[dict] = []

    # Source-Rows werden lazy einmal pro Commit-Request geladen und dann
    # für alle Sync-Aufrufe wiederverwendet. Bei 500 Buchungen spart das
    # 500 Full-Sheet-Reads.
    source_rows_cache: Optional[list[list[Any]]] = None

    def _get_source_rows_cache() -> list[list[Any]]:
        nonlocal source_rows_cache
        if source_rows_cache is None:
            source_rows_cache = _get_values(
                svc, cfg.spreadsheet_id, f"{cfg.source_tab}!{cfg.source_range_a1}"
            )
        return source_rows_cache

    logger.info(
        "commit start tenant=%s rows=%d max_rows=%d dry_run=%s",
        cfg.tenant, len(rows), max_rows, dry_run,
    )

    for idx, r in enumerate(rows):
        if max_rows > 0 and booked >= max_rows:
            break

        review_sheet_row = idx + 2
        rr = _read_review_row_values(r)
        status = rr["status"]

        if status != STATUS_AUTOBUCHUNG:
            skipped += 1
            continue

        try:
            vdate = _parse_sheet_date(rr["value_date_raw"])
            amount_cents = _parse_amount_to_cents(rr["amount_raw"])
            purpose = rr["purpose"]
            invoice_no = rr["invoice_no"]

            source_row_val = rr["source_row_a"] if (rr["source_row_a"] and str(rr["source_row_a"]).strip()) else rr["source_row_h"]
            source_row_hint: Optional[int] = None
            if source_row_val and str(source_row_val).strip().isdigit():
                source_row_hint = int(str(source_row_val).strip())

            if not invoice_no:
                errors += 1
                details.append({
                    "review_row": review_sheet_row,
                    "error": "missing invoice_no in review row",
                    "tenant": cfg.tenant,
                })
                continue

            if amount_cents <= 0:
                skipped += 1
                details.append({
                    "review_row": review_sheet_row,
                    "reason": "non_positive_amount",
                    "tenant": cfg.tenant,
                })
                continue

            marker = _make_fallback_marker(vdate, amount_cents, purpose)

            if dry_run:
                booked += 1
                details.append({
                    "review_row": review_sheet_row,
                    "invoice": invoice_no,
                    "marker": marker,
                    "tenant": cfg.tenant,
                    "dry_run": True,
                })
                continue

            try:
                p = _book_payment_to_patti(cfg, invoice_no, amount_cents, vdate, purpose, marker)
            except HTTPException as he:
                if he.status_code == 409:
                    msg = str(he.detail or "").lower()

                    if "multiple versions" in msg:
                        skipped += 1
                        _set_review_status_with_fallback(
                            svc,
                            cfg,
                            review_sheet_row,
                            STATUS_BEIHILFE_UNBUCHBAR,
                            fallback_status=STATUS_BEIHILFE_UNBUCHBAR,
                            fallback_prefix_in_purpose="BEIHILFE (unbuchbar) — ",
                        )
                        sync_info = _sync_source_status_if_possible(
                            cfg,
                            svc,
                            review_sheet_row=review_sheet_row,
                            review_vdate=vdate,
                            review_amount_cents=amount_cents,
                            review_purpose=purpose,
                            source_row_hint=source_row_hint,
                            target_status=STATUS_BEIHILFE_UNBUCHBAR,
                            source_rows_cache=_get_source_rows_cache(),
                        )
                        details.append({
                            "review_row": review_sheet_row,
                            "invoice": invoice_no,
                            "marker": marker,
                            "skipped": True,
                            "reason": "beihilfe_multiple_versions",
                            "tenant": cfg.tenant,
                            **sync_info,
                        })
                        continue

                    if "duplicate" in msg or "marker" in msg:
                        _update_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!B{review_sheet_row}", [[STATUS_GEBUCHT]])
                        sync_info = _sync_source_status_if_possible(
                            cfg,
                            svc,
                            review_sheet_row=review_sheet_row,
                            review_vdate=vdate,
                            review_amount_cents=amount_cents,
                            review_purpose=purpose,
                            source_row_hint=source_row_hint,
                            target_status=STATUS_GEBUCHT,
                            source_rows_cache=_get_source_rows_cache(),
                        )
                        skipped += 1
                        details.append({
                            "review_row": review_sheet_row,
                            "invoice": invoice_no,
                            "marker": marker,
                            "skipped": True,
                            "reason": "duplicate_marker_marked_as_booked",
                            "tenant": cfg.tenant,
                            **sync_info,
                        })
                        continue

                    if "multiple invoices found" in msg:
                        skipped += 1
                        _set_review_status_with_fallback(
                            svc,
                            cfg,
                            review_sheet_row,
                            STATUS_UNSICHER,
                            fallback_status=STATUS_UNSICHER,
                            fallback_prefix_in_purpose="UNSICHER (mehrere Rechnungen) — ",
                        )
                        details.append({
                            "review_row": review_sheet_row,
                            "invoice": invoice_no,
                            "marker": marker,
                            "skipped": True,
                            "reason": "multiple_invoices_same_number",
                            "tenant": cfg.tenant,
                        })
                        continue

                raise

            _update_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!B{review_sheet_row}", [[STATUS_GEBUCHT]])

            sync_info = _sync_source_status_if_possible(
                cfg,
                svc,
                review_sheet_row=review_sheet_row,
                review_vdate=vdate,
                review_amount_cents=amount_cents,
                review_purpose=purpose,
                source_row_hint=source_row_hint,
                target_status=STATUS_GEBUCHT,
                source_rows_cache=_get_source_rows_cache(),
            )

            booked += 1
            details.append({
                "review_row": review_sheet_row,
                "invoice": invoice_no,
                "payment_id": p.get("payment_id"),
                "marker": marker,
                "tenant": cfg.tenant,
                **sync_info,
            })

        except Exception as e:
            errors += 1
            logger.exception(
                "commit row error tenant=%s review_row=%d: %s", cfg.tenant, review_sheet_row, e,
            )
            details.append({
                "review_row": review_sheet_row,
                "error": str(e),
                "tenant": cfg.tenant,
            })

    logger.info(
        "commit done tenant=%s booked=%d skipped=%d errors=%d",
        cfg.tenant, booked, skipped, errors,
    )

    return CommitResponse(ok=True, booked=booked, skipped=skipped, errors=errors, details=details)
