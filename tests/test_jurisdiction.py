"""
State resolution — the thing that decides which grid factor a site is charged at.

Written because every record in every entity was being priced at Victoria's 0.78
regardless of where the site actually is. These tests pin the factor-bearing
behaviour, not the string handling: each case here is a number on a client's
report.
"""
import pytest

from services.jurisdiction import (
    VALID_JURISDICTIONS,
    postcode_to_state,
    resolve_jurisdiction,
    state_from_address,
)

# NGA 2025 location-based electricity, straight off the factor pack. Present so a
# test failure says what it costs, not just that a string changed.
EF = {"VIC": 0.78, "NSW": 0.64, "ACT": 0.64, "QLD": 0.67, "SA": 0.22,
      "TAS": 0.20, "WA_SWIS": 0.50, "WA_NWIS": 0.56, "NT_DKIS": 0.56}


# --- real-shaped addresses --------------------------------------------------

@pytest.mark.parametrize("address,expected", [
    ("123 Nepean Highway, Frankston VIC 3199", "VIC"),
    ("Level 4, 11 Herring Road, Macquarie Park NSW 2113", "NSW"),
    ("Unit 2/88 Logan Road, Woolloongabba QLD 4102", "QLD"),
    ("45 King William Street, Adelaide SA 5000", "SA"),
    ("200 St Georges Terrace, Perth WA 6000", "WA"),
    ("1 Davey Street, Hobart TAS 7000", "TAS"),
    ("10 Smith Street, Darwin NT 0800", "NT"),
    ("2 Constitution Avenue, Canberra ACT 2601", "ACT"),
])
def test_the_state_is_read_off_the_address(address, expected):
    state, source = state_from_address(address)
    assert state == expected
    assert source == "state_token"


@pytest.mark.parametrize("address,expected", [
    ("123 Nepean Highway, Frankston 3199", "VIC"),
    ("Level 4, 11 Herring Road, Macquarie Park 2113", "NSW"),
    ("45 King William Street, Adelaide 5000", "SA"),
    ("1 Davey Street, Hobart 7000", "TAS"),
])
def test_the_postcode_carries_it_when_the_state_is_missing(address, expected):
    state, source = state_from_address(address)
    assert state == expected
    assert source == "postcode"


def test_a_written_state_beats_a_postcode_they_disagree_with():
    """A transposed postcode is commoner than a wrong state."""
    state, source = state_from_address("1 Example St, Somewhere VIC 2113")
    assert (state, source) == ("VIC", "state_token")


def test_a_street_named_after_a_state_does_not_win():
    """'Western Australia Street, Richmond VIC' is in Victoria."""
    state, _ = state_from_address("5 Western Australia Street, Richmond VIC 3121")
    assert state == "VIC"


@pytest.mark.parametrize("address", [
    "frankston vic 3199", "FRANKSTON VIC 3199", "  Frankston   Vic   3199  ",
])
def test_casing_and_spacing_do_not_matter(address):
    assert state_from_address(address)[0] == "VIC"


def test_spelled_out_states_resolve():
    assert state_from_address("1 Bourke St, Melbourne, Victoria 3000")[0] == "VIC"
    assert state_from_address("1 George St, Sydney, New South Wales")[0] == "NSW"


# --- postcode ranges --------------------------------------------------------

@pytest.mark.parametrize("pc,expected", [
    ("2000", "NSW"), ("1234", "NSW"), ("2599", "NSW"), ("2619", "NSW"), ("2999", "NSW"),
    ("2601", "ACT"), ("2600", "ACT"), ("2618", "ACT"), ("2911", "ACT"), ("0200", "ACT"),
    ("3000", "VIC"), ("3999", "VIC"), ("8000", "VIC"), ("8999", "VIC"),
    ("4000", "QLD"), ("4999", "QLD"), ("9000", "QLD"), ("9999", "QLD"),
    ("5000", "SA"), ("5999", "SA"),
    ("6000", "WA"), ("6999", "WA"),
    ("7000", "TAS"), ("7999", "TAS"),
    ("0800", "NT"), ("0999", "NT"),
])
def test_postcode_allocation(pc, expected):
    assert postcode_to_state(pc) == expected


def test_act_blocks_are_carved_out_of_the_nsw_span():
    """2600-2618 and 2900-2920 are ACT even though they sit inside NSW's range."""
    assert postcode_to_state("2605") == "ACT"
    assert postcode_to_state("2598") == "NSW"
    assert postcode_to_state("2905") == "ACT"
    assert postcode_to_state("2921") == "NSW"


@pytest.mark.parametrize("junk", ["", "   ", None, "abcd", "12", "123456", "99999"])
def test_nonsense_postcodes_resolve_to_nothing(junk):
    assert postcode_to_state(junk) is None


# --- the resolver the ETL calls --------------------------------------------

@pytest.mark.parametrize("address,grid", [
    ("1 Nepean Hwy, Frankston VIC 3199", "VIC"),
    ("11 Herring Rd, Macquarie Park NSW 2113", "NSW"),
    ("45 King William St, Adelaide SA 5000", "SA"),
])
def test_resolve_gives_a_grid_the_factor_pack_prices(address, grid):
    j = resolve_jurisdiction(address)
    assert j.resolved and j.grid == grid
    assert j.grid in VALID_JURISDICTIONS
    assert j.grid in EF


def test_wa_resolves_to_a_grid_not_a_state():
    """
    The pack has no statewide WA factor. Returning plain 'WA' would build
    EF_ELEC_WA_LB_2025, which is not in the pack, and the adapter would skip the
    record silently - a site that vanishes rather than one that is priced wrong.
    """
    j = resolve_jurisdiction("200 St Georges Tce, Perth WA 6000")
    assert j.state == "WA"
    assert j.grid == "WA_SWIS"
    assert j.assumed_grid is True


def test_nt_resolves_to_a_grid_too():
    j = resolve_jurisdiction("10 Smith St, Darwin NT 0800")
    assert (j.state, j.grid, j.assumed_grid) == ("NT", "NT_DKIS", True)


def test_every_grid_the_resolver_can_emit_is_priced():
    for addr in ["Frankston VIC 3199", "Sydney NSW 2000", "Canberra ACT 2601",
                 "Brisbane QLD 4000", "Adelaide SA 5000", "Hobart TAS 7000",
                 "Perth WA 6000", "Darwin NT 0800"]:
        j = resolve_jurisdiction(addr)
        assert j.grid in VALID_JURISDICTIONS, addr
        assert j.grid in EF, addr


# --- the point of the whole exercise ---------------------------------------

def test_an_unresolvable_address_does_not_quietly_become_victoria():
    """
    The bug being fixed. An absent state used to inherit the adapter's pilot
    default of VIC and overstate an SA site by 255%. Unresolved must stay
    unresolved so the caller has to make a decision.
    """
    for bad in ["", "   ", None, "PO Box 123", "address unknown"]:
        j = resolve_jurisdiction(bad)
        assert j.resolved is False
        assert j.grid is None
        assert j.source == "unresolved"


def test_a_known_state_can_be_passed_in_when_the_address_is_useless():
    j = resolve_jurisdiction("PO Box 123", fallback_state="QLD")
    assert j.grid == "QLD" and j.source == "fallback_state"


def test_the_address_still_wins_over_the_fallback():
    j = resolve_jurisdiction("1 Bourke St, Melbourne VIC 3000", fallback_state="QLD")
    assert j.grid == "VIC" and j.source == "state_token"


def test_a_junk_fallback_is_not_trusted_either():
    assert resolve_jurisdiction(None, fallback_state="Narnia").resolved is False


def test_centurion_spanning_states_gets_different_factors():
    """One banner, several ABNs, several states - the case that broke."""
    sites = {
        "Frankston VIC 3199": 0.78,
        "Macquarie Park NSW 2113": 0.64,
        "Adelaide SA 5000": 0.22,
        "Woolloongabba QLD 4102": 0.67,
    }
    got = {a: EF[resolve_jurisdiction(a).grid] for a in sites}
    assert got == sites
    assert len(set(got.values())) == 4, "all four priced the same - the bug is back"
