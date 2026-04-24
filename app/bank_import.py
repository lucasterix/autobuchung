from __future__ import annotations

import hashlib
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


def _load_cfg(request: Request) -> TenantConfig:
    t = _get_tenant_from_request(request).upper()
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


# =========================================================
# API KEY GUARD
# =========================================================

def _require_api_key(cfg: TenantConfig, x_api_key: Optional[str]) -> None:
    if not cfg.bank_api_key:
        return
    if not x_api_key or x_api_key.strip() != cfg.bank_api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


# =========================================================
# HELPERS: Blank / Money / Dates / Marker
# =========================================================

def _is_blank_cell(v: Any) -> bool:
    if v is None:
        return True
    return str(v).strip() == ""


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00a0", " ")).strip()


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
# GOOGLE SHEETS CLIENT + RETRIES
# =========================================================

def _sheets_service(cfg: TenantConfig):
    creds = service_account.Credentials.from_service_account_file(
        cfg.google_application_credentials,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _is_retryable_google_error(e: Exception) -> bool:
    if isinstance(e, HttpError):
        status = getattr(e.resp, "status", None)
        return status in (429, 500, 502, 503, 504)
    return isinstance(e, (TimeoutError, ConnectionError))


def _retry(fn, *, tries: int = 6, base_sleep: float = 0.6, max_sleep: float = 6.0):
    last: Optional[Exception] = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if not _is_retryable_google_error(e) or i == tries - 1:
                raise
            sleep = min(max_sleep, base_sleep * (2 ** i)) * (0.75 + random.random() * 0.5)
            time_mod.sleep(sleep)
    if last:
        raise last
    raise RuntimeError("retry failed without exception")


def _get_values(svc, spreadsheet_id: str, range_a1: str) -> list[list[Any]]:
    def _do():
        sheet = svc.spreadsheets()
        res = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_a1).execute()
        return res.get("values", [])
    return _retry(_do)


def _update_values(svc, spreadsheet_id: str, range_a1: str, values: list[list[Any]]):
    def _do():
        sheet = svc.spreadsheets()
        return sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_a1,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
    return _retry(_do)


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
    return _retry(_do)


def _set_review_status_text_no_dropdown_needed(
    svc,
    cfg: TenantConfig,
    review_sheet_row: int,
    status_text: str,
    *,
    fallback_status: str = STATUS_UNSICHER,
    fallback_prefix_in_purpose: Optional[str] = None,
) -> None:
    try:
        _update_values(svc, cfg.spreadsheet_id, f"{cfg.review_tab}!B{review_sheet_row}", [[status_text]])
        return
    except HttpError:
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
            except Exception:
                pass


# =========================================================
# PATTI SESSION (per-tenant)
# =========================================================

_session_lock = threading.Lock()
_sessions: dict[str, requests.Session] = {}


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


def _login(cfg: TenantConfig, session: requests.Session) -> None:
    r = session.get(f"{cfg.patti_base}/login", timeout=cfg.http_timeout)
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

    r = session.post(
        f"{cfg.patti_base}/login",
        data=payload,
        headers=headers,
        allow_redirects=False,
        timeout=cfg.http_timeout,
    )
    if r.status_code not in (200, 302, 303):
        raise HTTPException(status_code=502, detail=f"Patti login failed: {r.status_code} {r.text[:200]}")

    if r.status_code in (302, 303) and r.headers.get("Location"):
        loc = r.headers["Location"]
        if loc.startswith("/"):
            loc = cfg.patti_base + loc
        session.get(loc, timeout=cfg.http_timeout)

    session.headers.update({"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
    xsrf = _get_xsrf_token(session)
    if xsrf:
        session.headers["X-XSRF-TOKEN"] = xsrf


def _get_session(cfg: TenantConfig) -> requests.Session:
    with _session_lock:
        s = _sessions.get(cfg.tenant)
        if s is None:
            s = _make_session()
            _login(cfg, s)
            _sessions[cfg.tenant] = s
        return s


def _request(cfg: TenantConfig, method: str, url: str, *, params=None, json=None) -> requests.Response:
    s = _get_session(cfg)
    r = s.request(method, url, params=params, json=json, timeout=cfg.http_timeout)

    if r.status_code in (401, 403, 419):
        with _session_lock:
            s2 = _make_session()
            _login(cfg, s2)
            _sessions[cfg.tenant] = s2
            s = s2
        r = s.request(method, url, params=params, json=json, timeout=cfg.http_timeout)

    return r


def _lookup_invoice_by_number(cfg: TenantConfig, invoice_no: str) -> dict:
    r = _request(cfg, "GET", f"{cfg.patti_base}/api/v1/invoices", params={"number": invoice_no})
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Invoice lookup failed: {r.status_code} {r.text[:200]}")
    data = r.json()
    items = data.get("data") or data.get("invoices") or []
    if not items:
        raise HTTPException(status_code=404, detail=f"Invoice not found: {invoice_no}")
    return items[0]


def _lookup_invoice_detail(cfg: TenantConfig, invoice_id: int) -> dict:
    r = _request(cfg, "GET", f"{cfg.patti_base}/api/v1/invoices/{invoice_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Invoice detail failed: {r.status_code} {r.text[:200]}")
    return r.json()


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
    payments = invoice_detail.get("payments") or []
    if isinstance(payments, list):
        for p in payments:
            note = str(p.get("note", "") or "")
            if f"[AUTOBOOK:{marker}]" in note:
                return True

    versions = invoice_detail.get("versions") or []
    if isinstance(versions, list):
        for v in versions:
            vp = v.get("payments") if isinstance(v, dict) else None
            if isinstance(vp, list):
                for p in vp:
                    note = str(p.get("note", "") or "")
                    if f"[AUTOBOOK:{marker}]" in note:
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
    return r.json()


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
    status = str(status_raw).replace("\u00a0", " ").strip() if status_raw is not None else ""

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

    status = str(status_raw).replace("\u00a0", " ").strip() if status_raw is not None else ""
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
) -> tuple[Optional[int], str]:
    if preferred_source_row:
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
) -> dict:
    matched_source_row, sync_reason = _find_matching_source_row_transactions(
        cfg,
        svc,
        review_vdate=review_vdate,
        review_amount_cents=review_amount_cents,
        review_purpose=review_purpose,
        preferred_source_row=source_row_hint,
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

@router.post("/import")
def import_bank_payment(request: Request, req: BankImportRequest, x_api_key: Optional[str] = Header(None)):
    cfg = _load_cfg(request)
    _require_api_key(cfg, x_api_key)

    marker = "legacy"
    result = _book_payment_to_patti(
        cfg=cfg,
        invoice_no=req.invoice_no,
        amount_cents=req.amount,
        value_date=req.value_date,
        purpose=req.purpose or "Bankimport",
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
            details.append({"row": sheet_row, "error": str(e), "tenant": cfg.tenant})

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
                        _set_review_status_text_no_dropdown_needed(
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
            details.append({
                "review_row": review_sheet_row,
                "error": str(e),
                "tenant": cfg.tenant,
            })

    return CommitResponse(ok=True, booked=booked, skipped=skipped, errors=errors, details=details)
