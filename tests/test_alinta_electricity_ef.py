from tools.alinta_electricity_ef import (
    apply_electricity_overrides,
    build_electricity_email_html,
    build_electricity_email_subject,
    compose_alinta_electricity_draft,
    flatten_electricity_draft_fields,
    format_nmis_for_subject,
    nmis_from_filename,
    N8N_AGREEMENT_TYPE,
    required_electricity_send_errors,
    split_nmis,
    _parse_ef_text,
)
from tools.bne_electricity_contracts import (
    lookup_bne_electricity_contract_from_rows,
    normalize_nmi,
)


def _extract(**overrides):
    base = {
        "company_name": "Frankston RSL",
        "acn_abn": "12345678901",
        "address": "PO BOX 3168, FRANKSTON EAST 3199",
        "tel": "8792 4400",
        "contact_name": "Brett Rowlands",
        "email": "browlands@frankstonrsl.com.au",
        "nmis": "VEEE0U1Y2S, 6407647367",
        "start_date": "1/11/2026",
        "end_date": "31/12/2029",
        "peak_rate_c_kwh": "18.50",
        "off_peak_rate_c_kwh": "12.20",
        "shoulder_rate_c_kwh": "15.00",
        "commission_c_kwh": "0.30",
        "annual_kwh": "1200000",
        "take_or_pay_pct": "",
        "is_signed": "true",
        "signed_date": "15/10/2026",
    }
    base.update(overrides)
    return base


def _sheet_lookup():
    return {
        "query_nmis": ["VEEE0U1Y2S", "6407647367"],
        "normalized_nmis": ["VEEE0U1Y2S", "6407647367"],
        "match_kind": "exact",
        "contracts": [
            {
                "nmi": "VEEE0U1Y2S",
                "company_name": "FRANKSTON RSL",
                "periods": [
                    {
                        "period_name": "Contract Period 1",
                        "peak_rate_c_kwh": 19.1,
                        "off_peak_rate_c_kwh": 12.5,
                        "shoulder_rate_c_kwh": 15.2,
                        "annual_kwh": 800000,
                    }
                ],
            }
        ],
    }


def test_split_nmis_rejects_heading_words():
    blob = (
        "ENGAGEMENT, ELECTRICITY, DISTRIBUTED, EXTRUSIONS, "
        "6203800971, 6203870325, DISTRIBUTOR, DISCLOSURE"
    )
    assert split_nmis(blob) == ["6203800971", "6203870325"]
    assert normalize_nmi("ENGAGEMENT") == ""
    assert normalize_nmi("ELECTRICITY") == ""
    assert normalize_nmi("DISCLOSURE") == ""
    assert normalize_nmi("0383489300") == ""


def test_split_nmis_excludes_abn():
    assert split_nmis("6203800971, 69109958018", exclude={"69109958018"}) == ["6203800971"]


def test_split_nmis_one_and_two():
    assert split_nmis("VEEE0U1Y2S") == ["VEEE0U1Y2S"]
    assert split_nmis("VEEE0U1Y2S, 6407647367") == ["VEEE0U1Y2S", "6407647367"]
    assert split_nmis("VEEE0U1Y2S & 6407647367") == ["VEEE0U1Y2S", "6407647367"]
    assert format_nmis_for_subject(["VEEE0U1Y2S", "6407647367"]) == "VEEE0U1Y2S & 6407647367"


def test_split_nmis_scans_full_form_heading_without_crash():
    blob = (
        "ENGAGEMENT FORM ELECTRICITY AGREEMENT DISTRIBUTED BY EGB EXECUTIVE "
        * 30
        + "NMI 6203800971 6203870325"
    )
    assert split_nmis(blob) == ["6203800971", "6203870325"]


def test_normalize_nmi_keeps_vic_letters():
    assert normalize_nmi("veee0u1y2s") == "VEEE0U1Y2S"
    assert normalize_nmi("6407647367") == "6407647367"
    assert normalize_nmi("6407647367.0") == "6407647367"


def test_nmis_from_filename():
    assert nmis_from_filename("Frankston RSL EF NMI VEEE0U1Y2S.pdf") == ["VEEE0U1Y2S"]


def test_sheet_hit_is_retention_and_uses_ef_rates():
    draft = compose_alinta_electricity_draft(_extract(peak_rate_c_kwh="18.50"), _sheet_lookup())
    assert draft["request_kind"] == "Retention"
    assert draft["fields"]["peak_rate_c_kwh"]["source"] == "ef"
    assert "18.50" in draft["fields"]["peak_rate_c_kwh"]["value"]


def test_draft_strips_heading_words_from_nmis():
    draft = compose_alinta_electricity_draft(
        _extract(
            nmis="ENGAGEMENT, ELECTRICITY, 6203800971, 6203870325, DISTRIBUTOR",
            shoulder_rate_c_kwh="",
        ),
        {"match_kind": "none", "contracts": []},
    )
    assert flatten_electricity_draft_fields(draft)["nmis"] == "6203800971, 6203870325"
    html = build_electricity_email_html(draft)
    assert "ENGAGEMENT" not in html
    assert "NMI: 6203800971" in html
    assert "NMI: 6203870325" in html
    assert "Shoulder:" not in html


def test_no_sheet_hit_is_acquisition():
    draft = compose_alinta_electricity_draft(_extract(), {"match_kind": "none", "contracts": []})
    assert draft["request_kind"] == "Acquisition"


def test_email_lists_both_nmis():
    html = build_electricity_email_html(
        compose_alinta_electricity_draft(_extract(), {"match_kind": "none", "contracts": []})
    )
    assert "VEEE0U1Y2S" in html
    assert "6407647367" in html
    assert "NMI: VEEE0U1Y2S" in html
    assert "NMI: 6407647367" in html
    assert "Peak:" in html
    assert "Alice" in html
    subject = build_electricity_email_subject(
        compose_alinta_electricity_draft(_extract(), {"match_kind": "none", "contracts": []})
    )
    assert "E-C&I" in subject
    assert "VEEE0U1Y2S" in subject
    assert "6407647367" in subject


def test_required_send_errors():
    draft = compose_alinta_electricity_draft(
        _extract(peak_rate_c_kwh="", commission_c_kwh=""),
        {"match_kind": "none", "contracts": []},
    )
    errors = required_electricity_send_errors(draft)
    assert any("Peak rate" in e for e in errors)
    assert any("Commission" in e for e in errors)
    assert any("Letter of Authority" in e for e in errors)


def test_manual_nmi_override():
    draft = compose_alinta_electricity_draft(_extract(nmis="VEEE0U1Y2S"), {"match_kind": "none", "contracts": []})
    updated = apply_electricity_overrides(draft, {"nmis": "VEEE0U1Y2S and 6407647367"})
    assert flatten_electricity_draft_fields(updated)["nmis"] == "VEEE0U1Y2S, 6407647367"


def test_n8n_agreement_type_is_not_signed_contract():
    assert N8N_AGREEMENT_TYPE == "alinta_electricity_agreement_request"
    assert N8N_AGREEMENT_TYPE not in {"contract", "contract_multiple_attachments", "eoi"}


def test_take_or_pay_defaults_to_80_and_uses_ef_70():
    draft = compose_alinta_electricity_draft(_extract(), {"match_kind": "none", "contracts": []})
    assert draft["fields"]["take_or_pay_pct"]["value"] == "80%"
    assert draft["fields"]["take_or_pay_pct"]["estimated"] is True
    assert "Take or Pay: 80%" in build_electricity_email_html(draft)

    draft70 = compose_alinta_electricity_draft(
        _extract(take_or_pay_pct="70"),
        {"match_kind": "none", "contracts": []},
    )
    assert draft70["fields"]["take_or_pay_pct"]["value"] == "70%"
    assert not draft70["fields"]["take_or_pay_pct"].get("estimated")
    assert "Take or Pay: 70%" in build_electricity_email_html(draft70)


def test_parse_take_or_pay_from_form_text():
    out = _parse_ef_text(
        "Company Name: Extrusions Australia\n"
        "NMI: 6203800971\n"
        "Peak Rate: 8.02\n"
        "Take or Pay: 70%\n"
        "Annual Consumption: 1200000"
    )
    assert out["take_or_pay_pct"] == "70"


def test_lookup_two_nmis_from_rows():
    rows = [
        {"NMI": "VEEE0U1Y2S", "Company Name": "FRANKSTON RSL", "Peak Rate (c/kWh)": "18.50"},
        {"NMI": "6407647367", "Company Name": "FRANKSTON RSL", "Peak Rate (c/kWh)": "19.10"},
        {"NMI": "41036565463", "Company Name": "OTHER", "Peak Rate (c/kWh)": "20.00"},
    ]
    first = lookup_bne_electricity_contract_from_rows("VEEE0U1Y2S", rows)
    assert first["match_kind"] == "exact"
    assert first["contracts"][0]["nmi"] == "VEEE0U1Y2S"
    second = lookup_bne_electricity_contract_from_rows("6407647367", rows)
    assert second["contracts"][0]["nmi"] == "6407647367"
