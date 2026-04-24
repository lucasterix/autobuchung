from datetime import date

import pytest

from app.bank_import import (
    _format_cents_eu_as_text,
    _load_cfg_for_tenant,
    _make_fallback_marker,
    _normalize_ws,
    _parse_amount_to_cents,
    _parse_retry_after,
    _parse_sheet_date,
    parse_purpose_for_invoice,
)


# ---------------------------------------------------------------------
# _parse_amount_to_cents
# ---------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("100,55", 10055),
    ("100.55", 10055),
    ("10,321", 1032100),           # en thousands → 10321 Euro = 1.032.100 Cent
    ("10.321,00", 1032100),        # de thousands + decimals
    ("1.000,00", 100000),
    ("1,000.00", 100000),
    ("0,01", 1),
    ("'123,45", 12345),            # Leading quote (Sheets Text-Erzwingung)
    ("  42,00 ", 4200),
    ("-5,00", -500),
    (0, 0),
    ("0", 0),
    ("1000", 100000),
])
def test_parse_amount_to_cents_ok(value, expected):
    assert _parse_amount_to_cents(value) == expected


@pytest.mark.parametrize("value", ["", None, "abc", "  ", "12,34,56"])
def test_parse_amount_to_cents_rejects_junk(value):
    with pytest.raises(ValueError):
        _parse_amount_to_cents(value)


# ---------------------------------------------------------------------
# _format_cents_eu_as_text
# ---------------------------------------------------------------------

@pytest.mark.parametrize("cents,expected", [
    (0, "'0,00"),
    (1, "'0,01"),
    (100, "'1,00"),
    (12345, "'123,45"),
    (1234500, "'12.345,00"),
    (-500, "'-5,00"),
    (-1234500, "'-12.345,00"),
])
def test_format_cents_eu(cents, expected):
    assert _format_cents_eu_as_text(cents) == expected


# ---------------------------------------------------------------------
# _parse_sheet_date
# ---------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("01.02.2026", date(2026, 2, 1)),
    ("1.2.2026", date(2026, 2, 1)),            # unpadded
    ("2026-02-01", date(2026, 2, 1)),
    ("2026-12-31T10:15:00", date(2026, 12, 31)),
])
def test_parse_sheet_date_ok(value, expected):
    assert _parse_sheet_date(value) == expected


@pytest.mark.parametrize("value", [None, "", "02/03/2026", "2026/02/03", "not a date"])
def test_parse_sheet_date_rejects(value):
    with pytest.raises(ValueError):
        _parse_sheet_date(value)


# ---------------------------------------------------------------------
# _normalize_ws (inkl. non-breaking space)
# ---------------------------------------------------------------------

def test_normalize_ws_collapses_spaces_and_nbsp():
    # " " = non-breaking space, häufig in Google-Sheets-Zellen
    assert _normalize_ws("  a  b c\t\nd   ") == "a b c d"


def test_normalize_ws_empty():
    assert _normalize_ws("") == ""
    assert _normalize_ws(None) == ""


# ---------------------------------------------------------------------
# _make_fallback_marker
# ---------------------------------------------------------------------

def test_marker_is_deterministic():
    m1 = _make_fallback_marker(date(2026, 2, 1), 12345, "Rechnung 1234")
    m2 = _make_fallback_marker(date(2026, 2, 1), 12345, "Rechnung 1234")
    assert m1 == m2
    assert len(m1) == 12


def test_marker_differs_for_different_inputs():
    a = _make_fallback_marker(date(2026, 2, 1), 12345, "Rechnung 1234")
    b = _make_fallback_marker(date(2026, 2, 2), 12345, "Rechnung 1234")
    c = _make_fallback_marker(date(2026, 2, 1), 99999, "Rechnung 1234")
    d = _make_fallback_marker(date(2026, 2, 1), 12345, "Rechnung 9999")
    assert len({a, b, c, d}) == 4


def test_marker_normalizes_whitespace():
    # Unterschiedliche Whitespaces im purpose ändern den Marker NICHT
    a = _make_fallback_marker(date(2026, 2, 1), 12345, "Re   1234")
    b = _make_fallback_marker(date(2026, 2, 1), 12345, "Re 1234")
    assert a == b


# ---------------------------------------------------------------------
# parse_purpose_for_invoice – Tenant A
# ---------------------------------------------------------------------

@pytest.fixture
def cfg_a():
    return _load_cfg_for_tenant("A")


@pytest.fixture
def cfg_b():
    return _load_cfg_for_tenant("B")


def test_tenant_a_single_invoice(cfg_a):
    r = parse_purpose_for_invoice("Rechnung 12345 Zahlung", cfg_a)
    assert r.invoice_no == "12345"
    assert r.skipped_reason is None


def test_tenant_a_four_digit_invoice(cfg_a):
    r = parse_purpose_for_invoice("Invoice 5678 please", cfg_a)
    assert r.invoice_no == "5678"


def test_tenant_a_skips_years(cfg_a):
    # Jahreszahlen 2000-2050 werden als Invoice-Kandidaten verworfen
    r = parse_purpose_for_invoice("Rechnung vom 2026 bezahlt", cfg_a)
    assert r.invoice_no is None
    assert r.skipped_reason == "no_candidate"


def test_tenant_a_strips_date_iban(cfg_a):
    # IBAN + Datum sollen die Invoice-Suche nicht verfälschen
    r = parse_purpose_for_invoice(
        "Zahlung 12.03.2026 IBAN DE89370400440532013000 Rechnung 3456", cfg_a
    )
    assert r.invoice_no == "3456"


def test_tenant_a_skips_5digit_not_starting_with_1(cfg_a):
    # Regel Tenant A: 5-stellig, erste Ziffer != 1 → ignorieren
    r = parse_purpose_for_invoice("Rechnung 23456", cfg_a)
    assert r.invoice_no is None


def test_tenant_a_multiple_candidates(cfg_a):
    r = parse_purpose_for_invoice("Rechnung 1234 und 5678", cfg_a)
    assert r.invoice_no is None
    assert r.skipped_reason == "multiple_candidates_skip"
    assert set(r.candidates) == {"1234", "5678"}


def test_tenant_a_blocklist(cfg_a):
    r = parse_purpose_for_invoice("Fahrtkostenerstattung 1234", cfg_a)
    assert r.invoice_no is None
    assert r.skipped_reason == "blocked_purpose"


# ---------------------------------------------------------------------
# parse_purpose_for_invoice – Tenant B
# ---------------------------------------------------------------------

def test_tenant_b_only_accepts_5digit_starting_with_7_8_9(cfg_b):
    assert parse_purpose_for_invoice("Rechnung 78123", cfg_b).invoice_no == "78123"
    assert parse_purpose_for_invoice("Rechnung 89123", cfg_b).invoice_no == "89123"
    assert parse_purpose_for_invoice("Rechnung 12345", cfg_b).invoice_no is None
    assert parse_purpose_for_invoice("Rechnung 67890", cfg_b).invoice_no is None


def test_tenant_b_does_not_strip_dates_like_a(cfg_b):
    # Tenant B überspringt das Date-/IBAN-Stripping (cfg_a only)
    r = parse_purpose_for_invoice("Invoice 89999 from 01.02.2026", cfg_b)
    assert r.invoice_no == "89999"


# ---------------------------------------------------------------------
# _parse_retry_after
# ---------------------------------------------------------------------

@pytest.mark.parametrize("header,default,expected", [
    (None, 2.0, 2.0),
    ("", 2.0, 2.0),
    ("5", 2.0, 5.0),
    ("  10 ", 2.0, 10.0),
    ("abc", 2.0, 2.0),
    ("-5", 2.0, 0.0),
    ("9999", 2.0, 60.0),   # deckelung
])
def test_parse_retry_after(header, default, expected):
    assert _parse_retry_after(header, default) == expected
