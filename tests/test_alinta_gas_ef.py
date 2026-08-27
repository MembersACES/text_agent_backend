from tools.alinta_gas_ef import (
    _parse_ef_text,
    _sanitize_extract,
    apply_flat_overrides,
    build_email_html,
    build_email_subject,
    compose_alinta_gas_draft,
    extract_alinta_gas_ef,
    flatten_draft_fields,
    gas_ef_folder_id,
    mirn_from_filename,
    N8N_AGREEMENT_TYPE,
    normalize_commission_per_gj,
    required_send_errors,
)


def _extract(**overrides):
    base = {
        "company_name": "Extrusion Australia",
        "acn_abn": "69109958018",
        "address": "25 Andretti Court, Truganina, VIC, 3209",
        "tel": "(03) 8348 9300",
        "contact_name": "Rod Fiddes",
        "email": "Rod@extrusions.com.au",
        "mirn": "53307608558",
        "start_date": "1/11/2026",
        "end_date": "31/12/2029",
        "price_per_gj": "14.70",
        "commission_per_gj": "2.10",
        "cpq_gj": "19000",
        "min_cpq_gj": "15200",
        "min_cpq_pct": "80",
        "mdq_gj": "65",
        "retail_service_charge": "1.99",
        "overrun_rate": "10.00",
        "excess_cpq_price": "2.00",
        "is_signed": "true",
        "signed_date": "15/10/2026",
    }
    base.update(overrides)
    return base


def _sheet_lookup():
    return {
        "query_mrin": "53307608558",
        "normalized_mrin": "53307608558",
        "match_kind": "exact",
        "contracts": [
            {
                "mrin": "53307608558",
                "company_name": "EXTRUSION AUSTRALIA PTY LTD",
                "contract_start_date": "01/11/2026",
                "contract_end_date": "31/12/2029",
                "periods": [
                    {
                        "period_name": "Contract Period 1",
                        "period_start_date": "01/11/2026",
                        "period_end_date": "31/12/2029",
                        "energy_rate_per_gj": 14.7,
                        "energy_rate_display": "14.70 $/GJ",
                        "cpq_gj": 19000,
                        "maq_gj": 15200,
                        "maq_pct": 80,
                        "mdq_gj_per_day": 65,
                        "overrun_rate_per_gj": 10.0,
                        "excess_cpq_rate_per_gj": 2.0,
                    }
                ],
            }
        ],
    }


def test_mirn_from_filename():
    assert mirn_from_filename("Extrusion Australia EF MIRN53307608558 .pdf") == "53307608558"
    assert mirn_from_filename("something else.pdf") == ""


def test_sheet_hit_is_retention_and_uses_sheet_schedule():
    draft = compose_alinta_gas_draft(_extract(price_per_gj="99.99"), _sheet_lookup())
    assert draft["request_kind"] == "Retention"
    assert draft["fields"]["price_per_gj"]["source"] == "sheet"
    assert draft["fields"]["price_per_gj"]["value"].startswith("$14.70")
    assert draft["fields"]["cpq_gj"]["value"] == "19,000"
    assert draft["fields"]["min_cpq_gj"]["value"] == "15,200"
    assert draft["fields"]["company_name"]["source"] == "ef"
    assert draft["fields"]["company_name"]["value"] == "Extrusion Australia"
    assert draft["fields"]["start_date"]["source"] == "ef"
    assert "19,000" in build_email_subject(draft)
    assert "Retention" in build_email_subject(draft)


def test_period_dates_come_from_ef_not_sheet():
    draft = compose_alinta_gas_draft(_extract(start_date="TBC", end_date="TBC"), _sheet_lookup())
    assert draft["fields"]["start_date"]["value"] == "TBC"
    assert draft["fields"]["end_date"]["value"] == "TBC"
    assert draft["fields"]["start_date"]["source"] == "ef"
    assert draft["fields"]["price_per_gj"]["source"] == "sheet"


def test_empty_ef_dates_are_not_filled_from_sheet():
    draft = compose_alinta_gas_draft(_extract(start_date="", end_date=""), _sheet_lookup())
    assert draft["fields"]["start_date"]["value"] == ""
    assert draft["fields"]["start_date"]["source"] == "missing"
    assert draft["fields"]["end_date"]["source"] == "missing"


def test_sheet_miss_uses_ef_and_estimates_min_cpq():
    extract = _extract(min_cpq_gj="", min_cpq_pct="")
    lookup = {"match_kind": "none", "contracts": []}
    draft = compose_alinta_gas_draft(extract, lookup)
    assert draft["request_kind"] == "Acquisition"
    assert draft["fields"]["price_per_gj"]["source"] == "ef"
    assert draft["fields"]["min_cpq_pct"]["estimated"] is True
    assert draft["fields"]["min_cpq_pct"]["value"] == "80%"
    assert draft["fields"]["min_cpq_gj"]["estimated"] is True
    assert draft["fields"]["min_cpq_gj"]["value"] == "15,200"
    assert "Acquisition" in build_email_subject(draft)


def test_commission_from_distributor_rebate_label():
    from tools.bne_gas_contracts import parse_sheet_number

    text = """
    Company Name: Extrusions Australia Pty Ltd
    Period 1 Rate $: Per GJ  $14.70
    Distributor Rebate: $3 per GJ    Included
    Rebate Paid By: Supplier
    Estimated Annual Outcome $355,698.39 -> $328,853.23 per year
    """
    assert parse_sheet_number(normalize_commission_per_gj("Included", extra_text=text)) == 3.0
    parsed = _parse_ef_text(text)
    assert parse_sheet_number(parsed["commission_per_gj"]) == 3.0
    assert parse_sheet_number(normalize_commission_per_gj("$2.10")) == 2.10
    assert normalize_commission_per_gj("Included") == ""


def test_commission_never_comes_from_sheet():
    extract = _extract(commission_per_gj="")
    draft = compose_alinta_gas_draft(extract, _sheet_lookup())
    assert draft["fields"]["commission_per_gj"]["value"] == ""
    assert draft["fields"]["commission_per_gj"]["estimated"] is True


def test_retail_service_charge_defaults():
    extract = _extract(retail_service_charge="")
    draft = compose_alinta_gas_draft(extract, {"match_kind": "none", "contracts": []})
    assert draft["fields"]["retail_service_charge"]["value"] == "1.99"
    assert draft["fields"]["retail_service_charge"]["source"] == "default"


def test_send_requires_loa_and_commission():
    draft = compose_alinta_gas_draft(_extract(commission_per_gj=""), {"match_kind": "none", "contracts": []})
    errors = required_send_errors(draft)
    assert any("Commission" in e for e in errors)
    assert any("Letter of Authority" in e for e in errors)


def test_manual_overrides_and_flatten():
    draft = compose_alinta_gas_draft(_extract(), {"match_kind": "none", "contracts": []})
    updated = apply_flat_overrides(draft, {"commission_per_gj": "2.10", "request_kind": "Retention"})
    assert flatten_draft_fields(updated)["commission_per_gj"].endswith("2.10") or "2.10" in flatten_draft_fields(updated)["commission_per_gj"]
    assert updated["request_kind"] == "Retention"
    updated["loa_file_id"] = "abc123abc123abc123abc1"
    assert required_send_errors(updated) == []


def test_n8n_agreement_type_is_not_signed_contract():
    assert N8N_AGREEMENT_TYPE == "alinta_agreement_request"
    assert N8N_AGREEMENT_TYPE not in {"contract", "contract_multiple_attachments", "eoi"}


def test_gas_ef_folder_id_default(monkeypatch):
    monkeypatch.delenv("ALINTA_GAS_EF_FOLDER_ID", raising=False)
    assert gas_ef_folder_id() == "1rSZIYdEsPviuyC4xmwOuPqI8gte8hpHA"
    monkeypatch.setenv("ALINTA_GAS_EF_FOLDER_ID", "abcFolder")
    assert gas_ef_folder_id() == "abcFolder"


def test_email_html_includes_alice_fornrg_signature():
    html = build_email_html(compose_alinta_gas_draft(_extract(), {"match_kind": "none", "contracts": []}))
    assert "Kind regards" in html
    assert "Alice" in html
    assert "FORNRG Pty Ltd" in html
    assert "1300 938 638" in html
    assert "http://www.fornrg.com/" in html


def test_sanitize_drops_egb_and_label_junk():
    cleaned = _sanitize_extract(
        {
            "company_name": "Environmental Global Benefits",
            "address": "Tel:Contact",
            "tel": "Contact",
            "contact_name": "Company",
            "email": "27/08/2026",
            "mirn": "5324563544",
            "min_cpq_pct": "20%",
            "acn_abn": "732126029",
        },
        text="Distributor Rebate: $3.3 per GJ\nLoad Flex: 20%",
    )
    assert cleaned["company_name"] == ""
    assert cleaned["address"] == ""
    assert cleaned["tel"] == ""
    assert cleaned["contact_name"] == ""
    assert cleaned["email"] == ""
    assert cleaned["min_cpq_pct"] == ""
    assert cleaned["mirn"] == "5324563544"


def test_sanitize_keeps_member_identity():
    cleaned = _sanitize_extract(
        {
            "company_name": "Frankston RSL Sub Branch Inc",
            "address": "Lot 1 183 CRANBOURNE Road FRANKSTON VIC 3199",
            "tel": "8792 4400",
            "contact_name": "Brett Rowlands",
            "email": "browlands@frankstonrsl.com.au",
            "mirn": "53215687544",
            "min_cpq_pct": "20",
        },
        text="Load Flex: 20%\nMIRN: 53215687544",
    )
    assert cleaned["company_name"] == "Frankston RSL Sub Branch Inc"
    assert cleaned["email"] == "browlands@frankstonrsl.com.au"
    assert cleaned["mirn"] == "53215687544"
    assert cleaned["min_cpq_pct"] == ""


def test_frankston_aces_gas_ef_layout():
    from pathlib import Path

    path = Path(r"c:\Users\morga\Downloads\698ecc52-e8aa-481d-a6d8-9bc659f75d2d (1).pdf")
    if not path.exists():
        return
    result = extract_alinta_gas_ef(path.read_bytes(), filename=path.name)
    extract = result["extract"]
    assert "Frankston" in extract["company_name"]
    assert "Environmental Global" not in extract["company_name"]
    assert extract["mirn"] == "53215687544"
    assert extract["email"] == "browlands@frankstonrsl.com.au"
    assert extract["contact_name"] == "Brett Rowlands"
    assert "CRANBOURNE" in extract["address"].upper()
    assert extract["acn_abn"] == "12643054953"
    assert "15.90" in extract["price_per_gj"] or extract["price_per_gj"] == "15.90"
    assert extract["commission_per_gj"] in {"3.3", "3.30"}
    assert extract["start_date"] in {"1/1/2028", "01/01/2028"}
    assert extract["end_date"] in {"31/12/2029"}
    assert extract["min_cpq_pct"] == ""
    assert not any("scanned pages" in w.lower() for w in result["extraction_warnings"])
