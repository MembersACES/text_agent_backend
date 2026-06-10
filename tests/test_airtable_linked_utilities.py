"""Tests for Oil/Waste LOA resolver and retailer fallback."""
from services import airtable_client


def _loa_with_oil_and_waste():
    return {
        "id": "recTestLoa",
        "fields": {
            "8th Sheet - Oil": ["recOil1"],
            "Link to Waste Clients": ["recWaste1"],
            "Retailers Oil Clients": ["Trojan Oils Pty Ltd"],
            "Retailers Waste Clients": ["J.J. Richards & Sons Pty Ltd"],
        },
    }


def test_get_linked_utility_records_includes_oil_and_waste(monkeypatch):
    def fake_fetch(table_name, rec_id):
        if table_name == "Oil Clients" and rec_id == "recOil1":
            return {
                "id": rec_id,
                "fields": {"Client Name": "FRANKSTON RSL", "Retailer": ""},
            }
        if table_name == "Waste Clients" and rec_id == "recWaste1":
            return {
                "id": rec_id,
                "fields": {
                    "Account Number or Customer Number": "09097571",
                    "Provider": "",
                },
            }
        return None

    monkeypatch.setattr(airtable_client, "_fetch_record", fake_fetch)
    linked, retailers, extra = airtable_client.get_linked_utility_records(_loa_with_oil_and_waste())

    assert "Oil" in linked
    assert linked["Oil"] == ["FRANKSTON RSL"]
    assert retailers["Oil"] == ["Trojan Oils Pty Ltd"]
    assert "Waste" in linked
    assert linked["Waste"] == ["09097571"]
    assert retailers["Waste"] == ["J.J. Richards & Sons Pty Ltd"]
    assert extra["Oil"][0]["retailer"] == "Trojan Oils Pty Ltd"
    assert extra["Waste"][0]["retailer"] == "J.J. Richards & Sons Pty Ltd"


def test_waste_link_field_fallback(monkeypatch):
    loa = {
        "id": "recTestLoa2",
        "fields": {
            "7th Sheet - Waste": ["recWaste2"],
            "Retailers Waste Clients": ["Provider Co"],
        },
    }

    def fake_fetch(table_name, rec_id):
        if table_name == "Waste Clients":
            return {
                "id": rec_id,
                "fields": {
                    "Account Number or Customer Number": "12345",
                    "Provider": "Provider Co",
                },
            }
        return None

    monkeypatch.setattr(airtable_client, "_fetch_record", fake_fetch)
    linked, retailers, _ = airtable_client.get_linked_utility_records(loa)
    assert linked.get("Waste") == ["12345"]
    assert retailers.get("Waste") == ["Provider Co"]
