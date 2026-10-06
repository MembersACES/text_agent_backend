"""
Which state a site is in, and therefore which grid factor applies to it.

This exists because of a real misstatement. The B4 adapter reads a per-record
`jurisdiction` and picks the matching NGA factor, but the ETL never set one, so
every record in every entity fell through to the adapter's pilot default of VIC
(0.78). That is right for Frankston, which is Victorian, and wrong for any
client with sites in more than one state. Centurion is one banner over many
ABNs across several states, and electricity is ~68% of its total, so the error
is not cosmetic:

    NSW/ACT 0.64 vs VIC 0.78   ->  22% overstated
    QLD     0.67               ->  16%
    WA SWIS 0.50               ->  56%
    SA      0.22               -> 255%
    TAS     0.20               -> 290%

The address is the fact. An identifier prefix only tells you the distributor,
which is an indirection and wrong at the edges, so it is not used here at all.

Resolution order, most to least trustworthy:
  1. An explicit state token in the address ("... RICHMOND VIC 3121")
  2. The postcode, via the Australia Post allocation ranges
  3. Nothing — the caller decides what to do, and must not quietly guess
"""
from __future__ import annotations

import re
from typing import Optional

# The jurisdictions the NGA 2025 pack actually prices. Note WA and NT have no
# statewide factor — the pack carries grid-level factors instead, so a WA
# address has to resolve to a grid or the adapter will look up a factor_key that
# does not exist and silently skip the record.
VALID_JURISDICTIONS = frozenset({
    "NSW", "ACT", "VIC", "QLD", "SA", "TAS", "WA_SWIS", "WA_NWIS", "NT_DKIS",
})

# Where a bare state maps when the pack has no statewide factor. SWIS covers
# Perth and the south-west; DKIS covers Darwin and Katherine. That is where
# commercial sites overwhelmingly are, but it IS an assumption, so a caller that
# cares should check `resolve_jurisdiction(...).assumed_grid`.
_GRID_DEFAULT = {"WA": "WA_SWIS", "NT": "NT_DKIS"}

_STATE_TOKENS = {
    "NSW": "NSW", "NEW SOUTH WALES": "NSW",
    "ACT": "ACT", "AUSTRALIAN CAPITAL TERRITORY": "ACT",
    "VIC": "VIC", "VICTORIA": "VIC",
    "QLD": "QLD", "QUEENSLAND": "QLD",
    "SA": "SA", "SOUTH AUSTRALIA": "SA",
    "WA": "WA", "WESTERN AUSTRALIA": "WA",
    "TAS": "TAS", "TASMANIA": "TAS",
    "NT": "NT", "NORTHERN TERRITORY": "NT",
}

# Australia Post allocations. Order matters: ACT's blocks sit inside NSW's
# numeric span, so they are tested first.
_POSTCODE_RANGES: list[tuple[int, int, str]] = [
    (200, 299, "ACT"),
    (800, 899, "NT"),
    (900, 999, "NT"),
    (1000, 2599, "NSW"),
    (2600, 2618, "ACT"),
    (2619, 2898, "NSW"),
    (2900, 2920, "ACT"),
    (2921, 2999, "NSW"),
    (3000, 3999, "VIC"),
    (4000, 4999, "QLD"),
    (5000, 5999, "SA"),
    (6000, 6999, "WA"),
    (7000, 7999, "TAS"),
    (8000, 8999, "VIC"),
    (9000, 9999, "QLD"),
]

_POSTCODE_RE = re.compile(r"\b(\d{4})\b")
_LONGEST_FIRST = sorted(_STATE_TOKENS, key=len, reverse=True)
_TOKEN_RE = re.compile(
    r"(?<![A-Za-z])(" + "|".join(re.escape(t) for t in _LONGEST_FIRST) + r")(?![A-Za-z])"
)


class Jurisdiction:
    """How a state was resolved, not just what it resolved to."""

    __slots__ = ("state", "grid", "source", "assumed_grid")

    def __init__(self, state: Optional[str], grid: Optional[str],
                 source: str, assumed_grid: bool = False):
        self.state = state          # "WA"      — plain state, or None
        self.grid = grid            # "WA_SWIS" — what the factor pack keys on
        self.source = source        # "state_token" | "postcode" | "unresolved"
        self.assumed_grid = assumed_grid

    @property
    def resolved(self) -> bool:
        return self.grid is not None

    def __repr__(self) -> str:  # pragma: no cover - debugging only
        return (f"Jurisdiction(state={self.state!r}, grid={self.grid!r}, "
                f"source={self.source!r}, assumed_grid={self.assumed_grid})")

    def __eq__(self, other) -> bool:
        if isinstance(other, Jurisdiction):
            return (self.state, self.grid, self.source, self.assumed_grid) == (
                other.state, other.grid, other.source, other.assumed_grid)
        return NotImplemented


def postcode_to_state(postcode) -> Optional[str]:
    """'3121' -> 'VIC'. None for anything outside the allocated ranges."""
    try:
        pc = int(str(postcode).strip())
    except (TypeError, ValueError):
        return None
    if not 0 <= pc <= 9999:
        return None
    for lo, hi, state in _POSTCODE_RANGES:
        if lo <= pc <= hi:
            return state
    return None


def state_from_address(address: Optional[str]) -> tuple[Optional[str], str]:
    """
    Pull a state out of a free-text Australian address.

    Returns (state, source). The state token wins over the postcode: a typed
    address is more likely to have a transposed postcode than a wrong state, and
    where they disagree the human-written state is the better evidence.
    """
    text = (address or "").strip().upper()
    if not text:
        return None, "unresolved"

    # Prefer the LAST token — "WESTERN AUSTRALIA STREET, RICHMOND VIC" should be
    # Victoria, and the state in an Australian address comes near the end.
    matches = _TOKEN_RE.findall(text)
    if matches:
        return _STATE_TOKENS[matches[-1]], "state_token"

    for pc in reversed(_POSTCODE_RE.findall(text)):
        state = postcode_to_state(pc)
        if state:
            return state, "postcode"

    return None, "unresolved"


def resolve_jurisdiction(address: Optional[str] = None,
                         fallback_state: Optional[str] = None) -> Jurisdiction:
    """
    The one call sites should use.

    `fallback_state` is for a state already known from elsewhere (the LOA's
    business-level State field, say) when the address yields nothing. It is NOT
    a default: pass None and an unresolvable address returns unresolved, so the
    caller has to decide rather than inherit someone's pilot setting.
    """
    state, source = state_from_address(address)

    if state is None and fallback_state:
        cand = str(fallback_state).strip().upper()
        if cand in _STATE_TOKENS:
            state, source = _STATE_TOKENS[cand], "fallback_state"

    if state is None:
        return Jurisdiction(None, None, "unresolved")

    grid = _GRID_DEFAULT.get(state, state)
    return Jurisdiction(
        state=state,
        grid=grid if grid in VALID_JURISDICTIONS else None,
        source=source,
        assumed_grid=state in _GRID_DEFAULT,
    )
