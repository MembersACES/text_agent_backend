"""Acceptance tests for services.sme_gas_invoice, from the 25 Sep 2026 invoice sheet sample."""

import pytest

from services.sme_gas_invoice import (
    classify,
    network_from_mirn,
    normalise_invoice,
    parse_period,
    parse_threshold,
    summarise_mirn,
)

BASIS = "c/MJ incl GST"

CAPORASO = {
    "MRIN": "55102454015", "Client Name": "CAPORASO PTY LTD", "Retailer": "Origin Energy",
    "Invoice Review Period": "27/03/2026-24/06/2026", "Invoice Review Number of Days": "90",
    "General Usage Quantity": "54511",
    "Block 1 Consumption": "54511", "Block 1 Rate": "4.5034", "Block 2 Rate": "3.2736", "Block 3 Rate": "2.8666",
    "Daily Supply Charge Quantity": "90", "Daily Supply Charge Rate": "1.461669",
    "Usage Discount %:": "14", "Supply Discount %:": "14",
    "Invoice Total:": "2351.27", "Invoice Number": "135101133", "Plan Name": "Business Select",
    "Price Change on Invoice": "N", "Rates Period": "27/03/2026-24/06/2026", "Rates Period Days": "90",
    "Block 1 Threshold": "First 88767", "Block 2 Threshold": "Next 88767 - 473424", "Block 3 Threshold": "Remaining",
    "Block 1 Amount $": "2454.85", "Block 2 Amount $": "0", "Block 3 Amount $": "0", "Daily Supply Amount $": "131.55",
    "GST $": "235.13", "Invoice Total incl GST": "2586.4", "Rates Basis": BASIS,
}

GLENROY = {
    "MRIN": "53302013763", "Invoice Review Period": "22/07/2025-16/09/2025", "Invoice Review Number of Days": "57",
    "Block 1 Consumption": "104500", "Block 1 Rate": "2.6268", "Block 2 Consumption": "41834.25", "Block 2 Rate": "2.2671",
    "Daily Supply Charge Quantity": "57", "Daily Supply Charge Rate": "1.015817",
    "Invoice Total:": "3410.29", "Invoice Number": "20394268", "Price Change on Invoice": "N",
    "Rates Period Days": "57", "Block 1 Threshold": "General usage", "Block 2 Threshold": "General usage next",
    "Block 1 Amount $": "2745.01", "Block 2 Amount $": "948.42", "Daily Supply Amount $": "57.89",
    "Invoice Total incl GST": "3751.32", "Rates Basis": BASIS,
}

LISCOMBE = {
    "MRIN": "53213722334", "Retailer": "EnergyAustralia", "Invoice Review Period": "11/04/2026-05/06/2026",
    "Invoice Review Number of Days": "56",
    "Block 1 Consumption": "2800", "Block 1 Rate": "4.7806", "Block 2 Consumption": "28000", "Block 2 Rate": "3.861",
    "Block 3 Consumption": "45920", "Block 3 Rate": "3.4815", "Block 4 Consumption": "464833.54", "Block 4 Rate": "3.0371",
    "Daily Supply Charge Quantity": "56", "Daily Supply Charge Rate": "1.3959",
    "Total Discount %:": "22", "Total Discount $:": "3742.04", "Invoice Total:": "12061.12",
    "Invoice Number": "119542110478", "Price Change on Invoice": "N", "Rates Period Days": "56",
    "Block 1 Threshold": "Business Balance Plan 12 Peak Consumption - Block 1 ( 50.00000 MJ/day)^",
    "Block 2 Threshold": "Business Balance Plan 12 Peak Consumption - Block 2 ( 500.00000 MJ/day)^",
    "Block 1 Amount $": "133.86", "Block 2 Amount $": "1081.08", "Block 3 Amount $": "1598.71", "Block 4 Amount $": "14117.46",
    "Daily Supply Amount $": "78.17", "Invoice Total incl GST": "13267.23", "Rates Basis": BASIS,
}

EAST_MALVERN = {
    "MRIN": "53102023079", "Invoice Review Period": "26/05/2026-27/07/2026", "Invoice Review Number of Days": "63",
    "General Usage Quantity": "28520",
    "Block 1 Consumption": "6750", "Block 1 Rate": "3.839", "Block 2 Consumption": "20250", "Block 2 Rate": "3.509",
    "Block 3 Consumption": "1520", "Block 3 Rate": "2.893",
    "Daily Supply Charge Quantity": "27", "Daily Supply Charge Rate": "1.30889",
    "Invoice Total:": "2132.46", "Invoice Number": "132584766", "Price Change on Invoice": "Y",
    "Rates Period": "01/07/2026-27/07/2026", "Rates Period Days": "27",
    "Block 1 Amount $": "259.13", "Block 2 Amount $": "710.57", "Block 3 Amount $": "43.97", "Daily Supply Amount $": "35.34",
    "Invoice Total incl GST": "2345.71", "Rates Basis": BASIS,
}


def _moama(period, days, b1, b2, b3, b4, a1, a2, a3, a4, supply, inc, number):
    return {
        "MRIN": "52210174393", "Invoice Review Period": period, "Invoice Review Number of Days": str(days),
        "Block 1 Consumption": b1, "Block 1 Rate": "3.641", "Block 2 Consumption": b2, "Block 2 Rate": "3.223",
        "Block 3 Consumption": b3, "Block 3 Rate": "2.816", "Block 4 Consumption": b4, "Block 4 Rate": "2.376",
        "Daily Supply Charge Quantity": str(days), "Daily Supply Charge Rate": "3.58952",
        "Invoice Number": number, "Price Change on Invoice": "N", "Rates Period Days": str(days),
        "Block 1 Amount $": a1, "Block 2 Amount $": a2, "Block 3 Amount $": a3, "Block 4 Amount $": a4,
        "Daily Supply Amount $": supply, "Invoice Total incl GST": inc, "Rates Basis": BASIS,
    }


MOAMA = [
    _moama("12/08/2025-09/10/2025", 59, "2950", "29500", "80830", "972278", "107.41", "950.79", "2276.17", "23101.33", "211.78", "26647.48", "113866479"),
    _moama("12/02/2026-15/04/2026", 63, "3150", "31500", "86310", "640628", "114.69", "1015.25", "2430.49", "15221.32", "226.14", "19007.89", "126342317"),
    _moama("10/10/2025-08/12/2025", 60, "3000", "30000", "82200", "700037", "109.23", "966.9", "2314.75", "16632.88", "215.37", "20239.13", "117912170"),
]


def test_period_without_spaces_parses():
    start, end = parse_period("27/03/2026-24/06/2026")
    assert (start.isoformat(), end.isoformat()) == ("2026-03-27", "2026-06-24")


def test_thresholds():
    assert parse_threshold("First 88767") == {"label": "First 88767", "upper_mj": 88767.0, "basis": "per_bill", "parsed": True}
    assert parse_threshold("Next 88767 - 473424")["upper_mj"] == 473424.0
    assert parse_threshold("Remaining")["basis"] == "remainder"
    ea = parse_threshold("Business Balance Plan 12 Peak Consumption - Block 1 ( 50.00000 MJ/day)^")
    assert (ea["upper_mj"], ea["basis"]) == (50.0, "per_day")
    assert parse_threshold("General usage")["parsed"] is False


def test_network_prefixes():
    assert network_from_mirn("55102454015")["id"] == "agn_sa"
    assert network_from_mirn("53202463627")["id"] == "agn_vic"
    assert network_from_mirn("53104321389")["id"] == "multinet"
    assert network_from_mirn("52210174393")["id"] is None


def test_caporaso():
    inv = normalise_invoice(CAPORASO)
    assert inv["usage_source"] == "blocks"
    assert inv["period_mj"] == 54511  # blocks only; General is not added on top
    assert inv["energy_rate_aud_per_gj"] == pytest.approx(45.034, abs=0.001)
    assert inv["all_in_aud_per_gj"] == pytest.approx(47.447, abs=0.001)
    assert inv["supply_aud_per_day"] == pytest.approx(1.4617, abs=0.0001)
    assert inv["annual_gj_this_invoice"] == pytest.approx(221.07, abs=0.01)
    assert inv["reconcile"]["status"] == "ok"
    assert inv["invoice_period"] == {"start": "2026-03-27", "end": "2026-06-24", "days": 90}
    assert inv["network"]["id"] == "agn_sa"
    assert inv["discounts"]["total_aud"] == 0  # 14% is not a credit line, never subtracted
    assert any(f["code"] == "discount_pct_only" for f in inv["flags"])


def test_glenroy_near_threshold():
    inv = normalise_invoice(GLENROY)
    assert inv["all_in_aud_per_gj"] == pytest.approx(25.635, abs=0.001)
    assert inv["reconcile"]["status"] == "ok"
    s = summarise_mirn([GLENROY])
    assert s["annual_gj"] == pytest.approx(937.1, abs=0.1)
    assert s["class"] == "sme"
    assert any(f["code"] == "near_ci_volume" for f in s["flags"])


def test_liscombe_discount_credit_and_ci_flag():
    inv = normalise_invoice(LISCOMBE)
    assert inv["period_mj"] == pytest.approx(541553.54)
    assert inv["discounts"]["total_aud"] == pytest.approx(3742.04)
    assert inv["all_in_aud_per_gj"] == pytest.approx(24.498, abs=0.001)
    assert inv["reconcile"]["status"] == "ok"
    assert inv["blocks"][0]["threshold"]["basis"] == "per_day"
    assert summarise_mirn([LISCOMBE])["class"] == "ci"


def test_east_malvern_price_change():
    inv = normalise_invoice(EAST_MALVERN)
    assert inv["price_change_on_invoice"] is True
    assert inv["reconcile"]["status"] == "not_checked_price_change"
    assert inv["annual_basis"]["source"] == "latest_period_mj_over_rates_days"
    assert inv["annual_basis"]["days"] == 27
    assert inv["supply"]["days"] == 27
    assert any(f["code"] == "price_change_no_whole_bill_mj" for f in inv["flags"])


def test_east_malvern_with_whole_bill_mj_uses_invoice_days():
    row = {**EAST_MALVERN, "Invoice Total MJ (whole bill)": "60000"}
    inv = normalise_invoice(row)
    assert inv["annual_basis"] == {"mj": 60000.0, "days": 63.0, "source": "whole_bill_mj_over_invoice_days"}
    assert inv["annual_gj_this_invoice"] == pytest.approx(60 * 365 / 63)


def test_moama_three_invoices_summed():
    s = summarise_mirn(MOAMA + [MOAMA[0]])  # duplicate is dropped
    assert s["invoice_count"] == 3
    assert s["coverage_days"] == 182
    assert s["annual_gj"] == pytest.approx(5339.4, abs=0.1)
    assert s["class"] == "ci"
    assert s["gaps"] == [{"from": "2025-12-09", "to": "2026-02-11"}]
    assert s["latest"]["invoice_number"] == "126342317"


def test_old_row_general_usage_fallback():
    row = {"MRIN": "53104321389", "Invoice Review Number of Days": "63", "General Usage Quantity": "71219.37",
           "General Usage Rate": "3.8852", "Daily Supply Charge Quantity": "63", "Daily Supply Charge Rate": "1.46113",
           "Invoice Total incl GST": "2859.06", "Rates Basis": BASIS}
    inv = normalise_invoice(row)
    assert inv["usage_source"] == "general_usage_fallback"
    assert inv["all_in_aud_per_gj"] == pytest.approx(40.144, abs=0.001)
    assert inv["reconcile"]["status"] == "ok"


def test_classify_boundary():
    assert classify(999.9)["class"] == "sme"
    assert classify(1000.0)["class"] == "ci"
