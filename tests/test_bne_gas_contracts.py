from tools.bne_gas_contracts import (
    lookup_bne_gas_contract_from_rows,
    mrin_match_kind,
    normalize_mrin,
    parse_sheet_number,
    select_matched_mrins,
)


def test_normalize_mrin_strips_noise():
    assert normalize_mrin("52409205958") == "52409205958"
    assert normalize_mrin(" 52 409 205 958 ") == "52409205958"
    assert normalize_mrin("52,409,205,958") == "52409205958"
    assert normalize_mrin("52409205958.0") == "52409205958"


def test_parse_sheet_number():
    assert parse_sheet_number("17.80 $/GJ") == 17.8
    assert parse_sheet_number("4,313 GJ") == 4313
    assert parse_sheet_number("80%") == 80
    assert parse_sheet_number("") is None
    assert parse_sheet_number(None) is None
    assert parse_sheet_number(22) == 22.0


def test_mrin_match_kinds():
    assert mrin_match_kind("52409205958", "52409205958") == "exact"
    assert mrin_match_kind("5240920595", "52409205958") == "checksum"
    assert mrin_match_kind("52409205958", "5240920595") == "checksum"
    assert mrin_match_kind("52409205958", "52409205957") == "one_digit"
    assert mrin_match_kind("52409205958", "99999999999") is None


def test_select_prefers_exact_over_checksum():
    kind, matched = select_matched_mrins(
        "52409205958",
        ["52409205958", "5240920595", "52409205957"],
    )
    assert kind == "exact"
    assert matched == ["52409205958"]


def test_select_checksum_when_invoice_missing_last_digit():
    kind, matched = select_matched_mrins("5240920595", ["52409205958", "53212243300"])
    assert kind == "checksum"
    assert matched == ["52409205958"]


def test_select_one_digit_when_last_digit_differs():
    kind, matched = select_matched_mrins("52409205957", ["52409205958", "53212243300"])
    assert kind == "one_digit"
    assert matched == ["52409205958"]


def _row(mrin: str, period: str, start: str, end: str, rate: str) -> dict:
    return {
        "MRIN": mrin,
        "Company Name": "RIVERWOOD LEGION AND COMMUNITY CLUB LIMITED",
        "Supply Address": "32 Littleton Street Riverwood NSW 2210",
        "Contract Start Date": "01/08/2026",
        "Contract End Date": "31/07/2028",
        "Period Start Date": start,
        "Period End Date": end,
        "Energy Rate ($/GJ)": rate,
        "CPQ (GJ)": "4,313 GJ",
        "MAQ (GJ)": "3,450 GJ",
        "MAQ (%)": "80%",
        "MDQ (GJ/day)": "22 GJ",
        "MHQ (GJ/hour)": "",
        "Overrun Rate ($/GJ)": "10.00 $/GJ",
        "Excess CPQ Rate ($/GJ)": "2.00 $/GJ",
        "VEEC Rate ($/Certificate)": "",
        "Period Name": period,
        "Retailer": "",
        "Webview Link": "https://drive.google.com/file/d/example/view",
    }


def test_lookup_groups_periods_and_sorts_period_1_first():
    rows = [
        _row("52409205958", "Contract Period 2", "01/08/2027", "31/07/2028", "17.80 $/GJ"),
        _row("52409205958", "Contract Period 1", "01/08/2026", "31/07/2027", "17.80 $/GJ"),
        _row("52400278747", "Contract Period 1", "01/08/2026", "31/07/2027", "17.80 $/GJ"),
    ]
    result = lookup_bne_gas_contract_from_rows("52409205958", rows)
    assert result["match_kind"] == "exact"
    assert len(result["contracts"]) == 1
    contract = result["contracts"][0]
    assert contract["mrin"] == "52409205958"
    assert contract["company_name"].startswith("RIVERWOOD")
    assert [p["period_name"] for p in contract["periods"]] == [
        "Contract Period 1",
        "Contract Period 2",
    ]
    assert contract["periods"][0]["energy_rate_per_gj"] == 17.8
    assert contract["periods"][0]["cpq_gj"] == 4313
    assert contract["periods"][0]["maq_pct"] == 80


def test_lookup_checksum_match_from_short_invoice_mrin():
    rows = [_row("52409205958", "Contract Period 1", "01/08/2026", "31/07/2027", "17.80 $/GJ")]
    result = lookup_bne_gas_contract_from_rows("5240920595", rows)
    assert result["match_kind"] == "checksum"
    assert result["contracts"][0]["mrin"] == "52409205958"


def test_lookup_no_match():
    rows = [_row("52409205958", "Contract Period 1", "01/08/2026", "31/07/2027", "17.80 $/GJ")]
    result = lookup_bne_gas_contract_from_rows("11111111111", rows)
    assert result["match_kind"] == "none"
    assert result["contracts"] == []
