"""Unit tests for contract-ending reporting enrichment."""
from types import SimpleNamespace

from services.airtable_client import (
    _first_text_from_fields,
    _loa_record_id_from_utility_fields,
)
from services.contract_ending import (
    classify_phone,
    enrich_contract_records,
    resolve_state,
    state_from_identifier,
)


def test_classify_phone_mobile_and_landline():
    assert classify_phone("0412 345 678") == "mobile"
    assert classify_phone("+61 412 345 678") == "mobile"
    assert classify_phone("03 9429 5111") == "landline"
    assert classify_phone("+61 3 9429 5111") == "landline"
    assert classify_phone("") == ""
    assert classify_phone("123") == "unknown"


def test_state_from_nmi_and_mirn():
    assert state_from_identifier("41036565463", "C&I Electricity") == "NSW"
    assert state_from_identifier("VEEE0WPKWT", "C&I Electricity") == "VIC"
    assert state_from_identifier("31123456789", "C&I Electricity") == "QLD"
    assert state_from_identifier("53001234567", "C&I Gas") == "VIC"
    assert state_from_identifier("52001234567", "C&I Gas") == "NSW"


def test_resolve_state_prefers_address_over_identifier():
    assert resolve_state(
        site_address="12 Smith St, Richmond VIC 3121",
        identifier="41036565463",
        utility_type="C&I Electricity",
    ) == "VIC"
    assert resolve_state(
        identifier="41036565463",
        utility_type="C&I Electricity",
    ) == "NSW"


def test_airtable_lookup_fields_extract_name_and_loa_id():
    fields = {
        "Bus Name Copy (from Link to LOA)": ["Acme Bakery"],
        "Link to LOA": ["recABCDEFGHIJK"],
        "Site Address": "1 George St Sydney 2000",
    }
    assert _first_text_from_fields(fields, ("Bus Name Copy (from Link to LOA)", "Business Name")) == "Acme Bakery"
    assert _loa_record_id_from_utility_fields(fields) == "recABCDEFGHIJK"


def test_airtable_lookup_uses_sheet_loa_link_and_skips_na_trading_as():
    fields = {
        "Trading As": ["N/A"],
        "Client Name": ["Selvan Naidoo"],
        "1st Sheet - LOA Business Details 3": ["recafZHjICWMdueoo"],
    }
    assert _first_text_from_fields(fields, ("Business Name", "Trading As")) == ""
    assert _loa_record_id_from_utility_fields(fields) == "recafZHjICWMdueoo"


def test_enrich_replaces_na_name_from_linked_loa():
    records = [
        {
            "identifier": "4311286487",
            "utility_type": "C&I Electricity",
            "contract_end_date": "2026-12-31",
            "retailer": "Stanwell Corporation Limited",
            "record_id": "recUtilNa",
            "business_name": "N/A",
            "loa_record_id": "recafZHjICWMdueoo",
            "site_address": "1 Eels Place, Parramatta, NSW, 2150",
            "state": "",
        }
    ]
    loa_records = [
        {
            "record_id": "recafZHjICWMdueoo",
            "business_name": "Parramatta Leagues Club Ltd",
            "trading_as": "N/A",
            "contact_name": "Selvan Naidoo",
            "email": "selvan.naidoo@parraleagues.com.au",
            "telephone": "0410 626 442",
            "site_address": "1 Eels Place, Parramatta, NSW, 2150",
            "postal_address": "",
            "state": "NSW",
        }
    ]
    clients = [
        SimpleNamespace(
            id=38,
            business_name="Parramatta Leagues Club Ltd",
            external_business_id="recafZHjICWMdueoo",
        )
    ]
    row = enrich_contract_records(records, loa_records, clients)[0]
    assert row["business_name"] == "Parramatta Leagues Club Ltd"
    assert row["contact_name"] == "Selvan Naidoo"
    assert row["email"] == "selvan.naidoo@parraleagues.com.au"
    assert row["telephone"] == "0410 626 442"
    assert row["phone_type"] == "mobile"
    assert row["client_id"] == 38
    assert row["portal_path"] == "/crm-members/38"


def test_enrich_joins_loa_contact_and_crm_portal():
    records = [
        {
            "identifier": "41036565463",
            "utility_type": "C&I Electricity",
            "contract_end_date": "2027-03-31",
            "retailer": "AGL",
            "record_id": "recUtil1",
            "business_name": "Acme Bakery",
            "loa_record_id": "recLOA1",
            "site_address": "",
            "state": "",
        }
    ]
    loa_records = [
        {
            "record_id": "recLOA1",
            "business_name": "Acme Bakery",
            "trading_as": "",
            "contact_name": "Jane Smith",
            "email": "jane@acme.test",
            "telephone": "0412 111 222",
            "site_address": "1 George St Sydney 2000",
            "postal_address": "",
            "state": "",
        }
    ]
    clients = [SimpleNamespace(id=42, business_name="Acme Bakery", external_business_id="recLOA1")]
    out = enrich_contract_records(records, loa_records, clients)
    assert len(out) == 1
    row = out[0]
    assert row["business_name"] == "Acme Bakery"
    assert row["state"] == "NSW"
    assert row["contact_name"] == "Jane Smith"
    assert row["email"] == "jane@acme.test"
    assert row["telephone"] == "0412 111 222"
    assert row["phone_type"] == "mobile"
    assert row["client_id"] == 42
    assert row["portal_path"] == "/crm-members/42"


def test_enrich_falls_back_to_member_search_without_crm_row():
    records = [
        {
            "identifier": "53000000001",
            "utility_type": "C&I Gas",
            "contract_end_date": None,
            "retailer": "",
            "record_id": "recUtil2",
            "business_name": "Solo Member Pty Ltd",
            "loa_record_id": "",
            "site_address": "",
            "state": "",
        }
    ]
    out = enrich_contract_records(records, [], [])
    assert out[0]["client_id"] is None
    assert "businessName=Solo%20Member%20Pty%20Ltd" in out[0]["portal_path"]
    assert out[0]["state"] == "VIC"
