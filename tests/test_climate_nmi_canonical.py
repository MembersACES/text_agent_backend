"""
NMI canonicalisation — one physical meter, one site.

An Australian NMI is 10 characters; an optional 11th digit is the AEMO
checksum. Both spellings circulate, and the LOA has been linking the same meter
under both. On Centurion that produced eight "sites" for four meters, and the
coverage report read as three empty sites plus several with missing months when
in fact one meter had a full twelve months of invoices and its twin had none.

These tests pin the real Centurion identifiers, because that is the evidence the
behaviour was derived from, and pin the cases that must NOT collapse.
"""
import pytest

from services.climate_activity_etl import canonical_site_id, nmi_check_digit
from services.climate_data_gaps import _site_key


# (10-char NMI, the 11-char form seen on the LOA) — all four from Centurion FY26
CENTURION_PAIRS = [
    ("2002323086", "20023230869"),
    ("4104120090", "41041200901"),
    ("4104143372", "41041433721"),
    ("VAAA000103", "VAAA0001036"),
]


@pytest.mark.parametrize("short,long", CENTURION_PAIRS)
def test_eleventh_character_is_the_aemo_check_digit(short, long):
    assert short + str(nmi_check_digit(short)) == long


@pytest.mark.parametrize("short,long", CENTURION_PAIRS)
def test_both_spellings_canonicalise_to_the_same_meter(short, long):
    assert canonical_site_id("C&I Electricity", long) == short
    assert canonical_site_id("C&I Electricity", short) == short


@pytest.mark.parametrize("short,long", CENTURION_PAIRS)
def test_both_spellings_group_as_one_site(short, long):
    assert _site_key("C&I Electricity", short) == _site_key("C&I Electricity", long)


def test_centurion_eight_links_are_four_meters():
    idents = [i for pair in CENTURION_PAIRS for i in pair]
    assert len({_site_key("C&I Electricity", i) for i in idents}) == 4


def test_sme_electricity_is_canonicalised_too():
    assert canonical_site_id("SME Electricity", "20023230869") == "2002323086"


# --- things that must be left alone ----------------------------------------

def test_wrong_check_digit_is_not_stripped():
    """Only a genuine checksum collapses. 2002323086's check digit is 9, not 1."""
    assert canonical_site_id("C&I Electricity", "20023230861") == "20023230861"


@pytest.mark.parametrize("utility", ["C&I Gas", "SME Gas", "Waste", "Oil"])
def test_non_electricity_identifiers_are_untouched(utility):
    """Gas MIRNs and account numbers don't use the NMI checksum scheme."""
    assert canonical_site_id(utility, "53237467421") == "53237467421"
    assert canonical_site_id(utility, "5323746742") == "5323746742"


def test_gas_mirns_from_centurion_are_unchanged():
    for mirn in ("5323746742", "5510254955"):
        assert canonical_site_id("C&I Gas", mirn) == mirn


def test_casing_and_whitespace_cannot_split_a_site():
    assert canonical_site_id("C&I Electricity", "  vaaa0001036 ") == "VAAA000103"
    assert _site_key("C&I Electricity", "vaaa000103") == _site_key(
        "C&I Electricity", "VAAA0001036"
    )


@pytest.mark.parametrize("bad", ["", "   ", None])
def test_empty_identifiers_survive(bad):
    assert canonical_site_id("C&I Electricity", bad) == ""


def test_short_and_long_identifiers_pass_through():
    assert canonical_site_id("C&I Electricity", "ABC") == "ABC"
    assert canonical_site_id("C&I Electricity", "123456789012345") == "123456789012345"


def test_check_digit_rejects_wrong_length():
    assert nmi_check_digit("12345") is None
    assert nmi_check_digit("123456789012") is None
